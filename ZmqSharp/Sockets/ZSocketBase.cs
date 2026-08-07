using System.Buffers;
using System.Collections.Concurrent;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared socket mechanics: calls the transport, stores connections, drives
/// the handshake externally, and delivers borrowed frames to the OnFrame
/// callback. Socket types are subtypes that differ only in
/// <see cref="RouteOutbound"/>.
/// </summary>
public abstract class ZSocketBase : ZAsyncState, IZCallbackSocket
{
    private static readonly byte[] ReadyCommand = [.. "READY\0"u8];

    private readonly record struct Peer(IZConnection Connection, object? Endpoint, ZmtpParser Parser);

    private readonly List<Peer> peers = [];
    private readonly List<(IZTransport Listener, object? Endpoint)> listeners = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];
    private ZFrameHandler? onFrame;
    private Func<IZConnection, ZFrameHandler>? frameSinkFactory;
    private Action<IZConnection, Exception?>? peerEnded;

    protected ZSocketBase(ZSocketOptions options)
        : base(options.Pool ?? MemoryPool<byte>.Shared)
    {
        ArgumentNullException.ThrowIfNull(options);
    }

    /// <summary>Selects the outbound connection(s) for a message; empty = drop.</summary>
    protected abstract IReadOnlyList<IZConnection> RouteOutbound(
        IZMessage message,
        IReadOnlyList<IZConnection> peers);

    public event ZFrameHandler? OnFrame
    {
        add
        {
            lock (StateLock)
            {
                onFrame += value;
            }
        }
        remove
        {
            lock (StateLock)
            {
                onFrame -= value;
            }
        }
    }

    public event Action<IZConnection, Exception?>? PeerEnded
    {
        add
        {
            lock (StateLock)
            {
                peerEnded += value;
            }
        }
        remove
        {
            lock (StateLock)
            {
                peerEnded -= value;
            }
        }
    }

    /// <summary>Resumes every peer receive pump paused by a false <see cref="OnFrame"/> return.</summary>
    public void ResumePaused()
    {
        while (paused.TryDequeue(out var parser))
        {
            parser.Resume();
        }
    }

    /// <summary>
    /// Replaces the shared OnFrame delivery with a per-peer sink factory.
    /// Must be called before any connection is established; each peer then
    /// gets its own frame handler.
    /// </summary>
    public void SetFrameSink(Func<IZConnection, ZFrameHandler> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        lock (StateLock)
        {
            if (peers.Count > 0)
            {
                throw new InvalidOperationException("frame sink must be set before connections are established");
            }

            frameSinkFactory = factory;
        }
    }

    public async Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var connection = await TTransport.ConnectAsync(endpoint, new ZTransportOptions(), token);
        AddConnection(connection, endpoint);
    }

    public async Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var listener = await TTransport.BindAsync(endpoint, new ZTransportOptions(), token);
        listener.OnAccept += AcceptConnection;
        lock (StateLock)
        {
            ThrowIfClosed();
            listeners.Add((listener, endpoint));
        }

        TrackBackground(listener.StartAsync(Cts.Token).AsTask());
    }

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        IZTransport? listener = null;
        lock (StateLock)
        {
            var index = listeners.FindIndex(entry => Equals(entry.Endpoint, endpoint));
            if (index >= 0)
            {
                listener = listeners[index].Listener;
                listeners.RemoveAt(index);
            }
        }

        listener?.Dispose();
        return Task.CompletedTask;
    }

    public Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        List<IZConnection> matches;
        lock (StateLock)
        {
            matches = [.. peers.Where(peer => Equals(peer.Endpoint, endpoint)).Select(peer => peer.Connection)];
        }

        foreach (var match in matches)
        {
            match.Dispose();
        }

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await AwaitBackgroundAsync();
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
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZBufferRef(owner, owner.Memory[..bytes.Length]));
        await SendAsync(message, token);
    }

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, null);
        return ValueTask.CompletedTask;
    }

    private void AddConnection(IZConnection connection, object? endpoint)
    {
        var parser = new ZmtpParser(connection, Pool);
        ZFrameHandler sink;
        lock (StateLock)
        {
            sink = frameSinkFactory?.Invoke(connection) ?? ((frame, ct) => RaiseOnFrame(frame));
        }

        connection.SetFrameHandler((frame, ct) => DeliverFrameCore(parser, frame, sink));

        lock (StateLock)
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
            await connection.WriteAsync(ZmtpFrameEncoder.NullGreeting, Cts.Token);
            await connection.SendCommandAsync(ReadyCommand, Cts.Token);
            if (await parser.EstablishAsync(Cts.Token))
            {
                await parser.ParseAsync(Cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ZeroMqProtocolException ex)
        {
            failure = ex;
            TrackBackground(StopAsync());
        }
        finally
        {
            connection.OnConnectionEnded();
            RaisePeerEnded(connection, failure);
            lock (StateLock)
            {
                peers.RemoveAll(peer => peer.Connection == connection);
            }
        }

        parser.Dispose();
        connection.Dispose();
    }

    private bool DeliverFrameCore(ZmtpParser parser, ZFrame frame, ZFrameHandler sink)
    {
        var keepGoing = sink(frame, Cts.Token);
        if (!keepGoing)
        {
            paused.Enqueue(parser);
        }

        return keepGoing;
    }

    private bool RaiseOnFrame(ZFrame frame)
    {
        ZFrameHandler? handler;
        lock (StateLock)
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
            keepGoing &= ((ZFrameHandler)item)(frame, Cts.Token);
        }

        return keepGoing;
    }

    private void RaisePeerEnded(IZConnection connection, Exception? failure)
    {
        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    private async Task StopAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0)
        {
            return;
        }

        await Cts.CancelAsync();
        List<IZTransport> listenerSnapshot;
        lock (StateLock)
        {
            listenerSnapshot = [.. listeners.Select(entry => entry.Listener)];
            listeners.Clear();
        }

        foreach (var listener in listenerSnapshot)
        {
            listener.Dispose();
        }

        Cts.Dispose();
    }

    private List<IZConnection> SnapshotPeers()
    {
        lock (StateLock)
        {
            return [.. peers.Select(peer => peer.Connection)];
        }
    }
}
