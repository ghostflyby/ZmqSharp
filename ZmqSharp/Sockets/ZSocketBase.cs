using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared socket mechanics: endpoint management, connection sessions, parser
/// integration, policy dispatch, two-layer send/receive, and backpressure.
/// </summary>
internal sealed class ZSocketBase : IZSocket
{
    private readonly IZSchedulingPolicy policy;
    private readonly MemoryPool<byte> pool;
    private readonly ZReceiveOptions receiveOptions;
    private readonly Lock stateLock = new();
    private readonly List<ZConnection> peers = [];
    private readonly List<(IZTransport Listener, string Endpoint)> listeners = [];
    private readonly ConcurrentQueue<ZConnection> paused = [];
    private readonly CancellationTokenSource cts = new();
    private readonly Channel<ZMessage>? receiveChannel;
    private readonly Channel<ZMessage>? sendChannel;
    private readonly Task? sendPump;
    private ZBorrowedMessageHandler? onMessage;
    private int closed;

    internal ZSocketBase(IZSchedulingPolicy policy, ZSocketOptions options)
    {
        this.policy = policy;
        pool = options.Pool ?? MemoryPool<byte>.Shared;
        receiveOptions = new ZReceiveOptions
        {
            Policy = ZReceiveMode.Borrowed,
            ContiguousFrameLimit = options.Receive.ContiguousFrameLimit,
        };

        if (options.ReceiveChannelCapacity is { } receiveCapacity)
        {
            receiveChannel = Channel.CreateBounded<ZMessage>(
                new BoundedChannelOptions(receiveCapacity) { SingleReader = true });
        }

        if (options.SendChannelCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<ZMessage>(
                new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(cts.Token);
        }
    }

    public event ZBorrowedMessageHandler? OnMessage
    {
        add
        {
            lock (stateLock)
            {
                onMessage += value;
            }
        }
        remove
        {
            lock (stateLock)
            {
                onMessage -= value;
            }
        }
    }

    public ChannelReader<ZMessage>? Messages => receiveChannel?.Reader;

    public ChannelWriter<ZMessage>? Outbound => sendChannel?.Writer;

    public async Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var transport = await TTransport.ConnectAsync(this, endpoint, token);
        AddConnection(transport, endpoint?.ToString() ?? string.Empty);
    }

    public async Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var listener = await TTransport.BindAsync(this, endpoint, token);
        lock (stateLock)
        {
            ThrowIfClosed();
            listeners.Add((listener, endpoint?.ToString() ?? string.Empty));
        }

        _ = AcceptLoopAsync(listener);
    }

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        string key = endpoint?.ToString() ?? string.Empty;
        IZTransport? listener = null;
        lock (stateLock)
        {
            int index = listeners.FindIndex(entry => entry.Endpoint == key);
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
        string key = endpoint?.ToString() ?? string.Empty;
        List<ZConnection> matches;
        lock (stateLock)
        {
            matches = [.. peers.Where(peer => peer.Endpoint == key)];
        }

        foreach (var match in matches)
        {
            match.Abort();
        }

        return Task.CompletedTask;
    }

    public async Task CloseAsync(CancellationToken token = default)
    {
        await StopAsync(token);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        ThrowIfClosed();
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            var targets = policy.RouteOutbound(message, SnapshotPeers());
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
        var message = ZMessageFactory.CopyToPooled(bytes, pool);
        await SendAsync(message, token);
    }

    public bool TrySend(ZMessage message)
    {
        ThrowIfClosed();
        return sendChannel?.Writer.TryWrite(message) ?? false;
    }

    internal bool Deliver(ZConnection peer, ZMessageView view)
    {
        if (receiveChannel is { } channel)
        {
            var message = policy.OnInbound(ZMessageFactory.Materialize(view, pool), peer);
            if (message is null)
            {
                return true;
            }

            if (channel.Writer.TryWrite(message))
            {
                return true;
            }

            message.Dispose();
            paused.Enqueue(peer);
            _ = ResumePausedAsync();
            return false;
        }

        ZBorrowedMessageHandler? handler;
        lock (stateLock)
        {
            handler = onMessage;
        }

        bool keepGoing = true;
        if (handler is not null)
        {
            foreach (Delegate item in handler.GetInvocationList())
            {
                keepGoing &= ((ZBorrowedMessageHandler)item)(view, cts.Token);
            }
        }

        return keepGoing;
    }

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        await foreach (var message in channel.Reader.ReadAllAsync(token))
        {
            await SendAsync(message, token);
        }
    }

    private async Task AcceptLoopAsync(IZTransport listener)
    {
        try
        {
            while (Volatile.Read(ref closed) == 0)
            {
                var transport = await listener.AcceptAsync(cts.Token);
                AddConnection(transport, string.Empty);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (IOException)
        {
        }
        finally
        {
            lock (stateLock)
            {
                listeners.RemoveAll(entry => entry.Listener == listener);
            }
        }
    }

    private void AddConnection(IZTransport transport, string endpoint)
    {
        var connection = new ZConnection(transport, receiveOptions, pool)
        {
            Endpoint = endpoint,
        };
        connection.Sink = new SocketSink(this, connection);

        lock (stateLock)
        {
            ThrowIfClosed();
            peers.Add(connection);
        }

        _ = RunConnectionAsync(connection);
    }

    private async Task RunConnectionAsync(ZConnection connection)
    {
        var runTask = connection.RunAsync(cts.Token);
        connection.RunTask = runTask;
        try
        {
            await runTask;
        }
        catch (OperationCanceledException)
        {
        }
        catch (ZeroMqProtocolException ex)
        {
            Fail(ex);
        }
        finally
        {
            lock (stateLock)
            {
                peers.Remove(connection);
            }
        }

        await connection.DisposeAsync();
    }

    private void Fail(ZeroMqProtocolException ex)
    {
        receiveChannel?.Writer.TryComplete(ex);
        sendChannel?.Writer.TryComplete(ex);
        _ = StopAsync(CancellationToken.None);
    }

    private async Task StopAsync(CancellationToken token)
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
        {
            return;
        }

        cts.Cancel();
        List<IZTransport> listeners;
        Task[] tasks;
        lock (stateLock)
        {
            listeners = [.. this.listeners.Select(entry => entry.Listener)];
            this.listeners.Clear();
            tasks = [.. peers.Select(peer => peer.RunTask ?? Task.CompletedTask)];
        }

        foreach (var listener in listeners)
        {
            listener.Dispose();
        }

        if (tasks.Length > 0)
        {
            try
            {
                await Task.WhenAll(tasks).WaitAsync(token);
            }
            catch (OperationCanceledException)
            {
                // Parser reads abort with cancellation during shutdown.
            }
        }

        receiveChannel?.Writer.TryComplete();
        sendChannel?.Writer.TryComplete();
        if (sendPump is not null)
        {
            try
            {
                await sendPump;
            }
            catch (OperationCanceledException)
            {
            }
        }

        cts.Dispose();
    }

    private List<ZConnection> SnapshotPeers()
    {
        lock (stateLock)
        {
            return [.. peers];
        }
    }

    private async Task ResumePausedAsync()
    {
        var channel = receiveChannel;
        if (channel is null)
        {
            return;
        }

        while (paused.TryDequeue(out var peer))
        {
            await channel.Writer.WaitToWriteAsync(cts.Token);
            peer.Resume();
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref closed) == 1)
        {
            throw new ObjectDisposedException(nameof(ZSocketBase));
        }
    }
}
