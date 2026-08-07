using System.Buffers;
using System.Collections.Concurrent;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared socket mechanics: calls the transport, stores connections, drives
/// the handshake externally, and delivers borrowed frames to the OnFrame
/// callback. Socket types differ only in <see cref="RouteOutbound"/>.
/// </summary>
internal abstract class ZSocketBase : IZSocket
{
    private static readonly byte[] ReadyCommand = [.. "READY\0"u8];

    private readonly record struct Peer(IZConnection Connection, string Endpoint, ZmtpParser Parser);

    private readonly MemoryPool<byte> pool;
    private readonly Lock stateLock = new();
    private readonly List<Peer> peers = [];
    private readonly List<(IZTransport Listener, string Endpoint)> listeners = [];
    private readonly List<Task> backgroundTasks = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];
    private readonly CancellationTokenSource cts = new();
    private ZFrameHandler? onFrame;
    private Action<Exception?>? peerEnded;
    private int closed;

    protected ZSocketBase(ZSocketOptions options)
    {
        pool = options.Pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>Selects the outbound connection(s) for a message; empty = drop.</summary>
    protected abstract IReadOnlyList<IZConnection> RouteOutbound(
        IZMessage message,
        IReadOnlyList<IZConnection> peers);

    public event ZFrameHandler? OnFrame
    {
        add
        {
            lock (stateLock)
            {
                onFrame += value;
            }
        }
        remove
        {
            lock (stateLock)
            {
                onFrame -= value;
            }
        }
    }

    public event Action<Exception?>? PeerEnded
    {
        add
        {
            lock (stateLock)
            {
                peerEnded += value;
            }
        }
        remove
        {
            lock (stateLock)
            {
                peerEnded -= value;
            }
        }
    }

    public async Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var connection = await TTransport.ConnectAsync(endpoint, new ZTransportOptions(), token);
        AddConnection(connection, endpoint?.ToString() ?? string.Empty);
    }

    public async Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var listener = await TTransport.BindAsync(endpoint, new ZTransportOptions(), token);
        listener.OnAccept += AcceptConnection;
        lock (stateLock)
        {
            ThrowIfClosed();
            listeners.Add((listener, endpoint?.ToString() ?? string.Empty));
        }

        TrackBackground(listener.StartAsync(cts.Token).AsTask());
    }

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        var key = endpoint?.ToString() ?? string.Empty;
        IZTransport? listener = null;
        lock (stateLock)
        {
            var index = listeners.FindIndex(entry => entry.Endpoint == key);
            if (index >= 0)
            {
                listener = listeners[index].Listener;
                listeners.RemoveAt(index);
            }
        }

        listener?.Dispose();
        return Task.CompletedTask;
    }

    public Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        var key = endpoint?.ToString() ?? string.Empty;
        List<IZConnection> matches;
        lock (stateLock)
        {
            matches = [.. peers.Where(peer => peer.Endpoint == key).Select(peer => peer.Connection)];
        }

        foreach (var match in matches)
        {
            match.Dispose();
        }

        return Task.CompletedTask;
    }

    public async Task CloseAsync(CancellationToken token = default)
    {
        await StopAsync(token);
        await AwaitBackgroundAsync(token);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync(CancellationToken.None);
    }

    public void ResumePaused()
    {
        while (paused.TryDequeue(out var parser))
        {
            parser.Resume();
        }
    }

    public async ValueTask SendAsync(IZMessage message, CancellationToken token = default)
    {
        ThrowIfClosed();
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            var targets = RouteOutbound(message, SnapshotPeers());
            foreach (var target in targets)
            {
                await target.SendAsync(message, token);
            }
        }
        finally
        {
            message.Dispose();
        }
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ThrowIfClosed();
        var owner = pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZBufferRef(owner, owner.Memory[..bytes.Length]));
        await SendAsync(message, token);
    }

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, string.Empty);
        return ValueTask.CompletedTask;
    }

    private void AddConnection(IZConnection connection, string endpoint)
    {
        var parser = new ZmtpParser(connection, pool);
        connection.SetFrameHandler((frame, ct) => DeliverFrame(parser, frame));

        lock (stateLock)
        {
            ThrowIfClosed();
            peers.Add(new Peer(connection, endpoint, parser));
        }

        TrackBackground(RunConnectionAsync(connection, parser));
    }

    private async Task RunConnectionAsync(IZConnection connection, ZmtpParser parser)
    {
        Exception? failure = null;
        try
        {
            await connection.WriteAsync(ZmtpFrameEncoder.NullGreeting, cts.Token);
            await connection.SendCommandAsync(ReadyCommand, cts.Token);
            if (await parser.EstablishAsync(cts.Token))
            {
                await parser.ParseAsync(cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ZeroMqProtocolException ex)
        {
            failure = ex;
            TrackBackground(StopAsync(CancellationToken.None));
        }
        finally
        {
            connection.OnConnectionEnded();
            RaisePeerEnded(failure);
            lock (stateLock)
            {
                peers.RemoveAll(peer => peer.Connection == connection);
            }
        }

        parser.Dispose();
        connection.Dispose();
    }

    private bool DeliverFrame(ZmtpParser parser, ZFrame frame)
    {
        var keepGoing = RaiseOnFrame(frame);
        if (!keepGoing)
        {
            paused.Enqueue(parser);
        }

        return keepGoing;
    }

    private bool RaiseOnFrame(ZFrame frame)
    {
        ZFrameHandler? handler;
        lock (stateLock)
        {
            handler = onFrame;
        }

        if (handler is null)
        {
            return true;
        }

        var keepGoing = true;
        foreach (Delegate item in handler.GetInvocationList())
        {
            keepGoing &= ((ZFrameHandler)item)(frame, cts.Token);
        }

        return keepGoing;
    }

    private void RaisePeerEnded(Exception? failure)
    {
        Action<Exception?>? handler;
        lock (stateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(failure);
    }

    private void TrackBackground(Task task)
    {
        lock (stateLock)
        {
            backgroundTasks.Add(task);
        }
    }

    private async Task AwaitBackgroundAsync(CancellationToken token)
    {
        Task[] tasks;
        lock (stateLock)
        {
            tasks = [.. backgroundTasks];
        }

        if (tasks.Length == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(tasks).WaitAsync(token);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ZeroMqProtocolException)
        {
        }
    }

    private async Task StopAsync(CancellationToken token)
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
        {
            return;
        }

        await cts.CancelAsync();
        List<IZTransport> listenerSnapshot;
        lock (stateLock)
        {
            listenerSnapshot = [.. listeners.Select(entry => entry.Listener)];
            listeners.Clear();
        }

        foreach (var listener in listenerSnapshot)
        {
            listener.Dispose();
        }

        cts.Dispose();
    }

    private List<IZConnection> SnapshotPeers()
    {
        lock (stateLock)
        {
            return [.. peers.Select(peer => peer.Connection)];
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref closed) != 1) return;
        throw new ObjectDisposedException(nameof(ZSocketBase));
    }
}
