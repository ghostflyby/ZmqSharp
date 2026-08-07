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
    private readonly Lock stateLock = new();
    private readonly List<ZConnection> peers = [];
    private readonly List<(IZTransport Listener, string Endpoint)> listeners = [];
    private readonly List<Task> backgroundTasks = [];
    private readonly ConcurrentQueue<ZConnection> paused = [];
    private readonly CancellationTokenSource cts = new();
    private readonly Channel<IZMessage>? receiveChannel;
    private readonly Channel<IZMessage>? sendChannel;
    private readonly Task? sendPump;
    private ZFrameHandler? onFrame;
    private int closed;

    internal ZSocketBase(IZSchedulingPolicy policy, ZSocketOptions options)
    {
        this.policy = policy;
        pool = options.Pool ?? MemoryPool<byte>.Shared;

        if (options.ReceiveChannelCapacity is { } receiveCapacity)
        {
            receiveChannel = Channel.CreateBounded<IZMessage>(
                new BoundedChannelOptions(receiveCapacity) { SingleReader = true });
        }

        if (options.SendChannelCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<IZMessage>(
                new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(cts.Token);
        }
    }

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

    public ChannelReader<IZMessage>? Messages => receiveChannel?.Reader;

    public ChannelWriter<IZMessage>? Outbound => sendChannel?.Writer;

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

        TrackBackground(AcceptLoopAsync(listener));
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
        await AwaitBackgroundAsync(token);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync(CancellationToken.None);
    }

    public async ValueTask SendAsync(IZMessage message, CancellationToken token = default)
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
        var owner = pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZBufferRef(owner, owner.Memory[..bytes.Length]));
        await SendAsync(message, token);
    }

    public bool TrySend(IZMessage message)
    {
        ThrowIfClosed();
        return sendChannel?.Writer.TryWrite(message) ?? false;
    }

    internal bool DeliverFrame(ZConnection peer, ZFrame frame, List<ZBufferRef> accumulator)
    {
        var keepGoing = RaiseOnFrame(frame);
        if (receiveChannel is not null)
        {
            keepGoing &= Accumulate(peer, frame, accumulator);
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

    private bool Accumulate(ZConnection peer, ZFrame frame, List<ZBufferRef> accumulator)
    {
        var owner = pool.Rent(frame.Memory.Length);
        frame.Memory.CopyTo(owner.Memory);
        accumulator.Add(new ZBufferRef(owner, owner.Memory[..frame.Memory.Length]));

        if (frame.More)
        {
            return true;
        }

        IZMessage message = accumulator.Count == 1
            ? new ZMessage(accumulator[0])
            : new ZMultiMessage([.. accumulator]);
        accumulator.Clear();

        if (receiveChannel!.Writer.TryWrite(message))
        {
            return true;
        }

        message.Dispose();
        paused.Enqueue(peer);
        TrackBackground(ResumePausedAsync());
        return false;
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
        var connection = new ZConnection(transport, pool)
        {
            Endpoint = endpoint,
        };
        connection.Sink = new SocketSink(this, connection);

        lock (stateLock)
        {
            ThrowIfClosed();
            peers.Add(connection);
        }

        TrackBackground(RunConnectionAsync(connection));
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
        TrackBackground(StopAsync(CancellationToken.None));
    }

    private async Task StopAsync(CancellationToken token)
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
        {
            return;
        }

        await cts.CancelAsync();
        List<IZTransport> listenerSnapshot;
        Task[] tasks;
        lock (stateLock)
        {
            listenerSnapshot = [.. listeners.Select(entry => entry.Listener)];
            listeners.Clear();
            tasks = [.. peers.Select(peer => peer.RunTask ?? Task.CompletedTask)];
        }

        foreach (var listener in listenerSnapshot)
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
            catch (ZeroMqProtocolException)
            {
                // Already surfaced via Fail; the raw parser task faults with it.
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
            // Expected during shutdown.
        }
        catch (ZeroMqProtocolException)
        {
            // Already surfaced via the receive channel by Fail.
        }
    }

    private List<ZConnection> SnapshotPeers()
    {
        lock (stateLock)
        {
            return [.. peers];
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref closed) != 1) return;
        throw new ObjectDisposedException(nameof(ZSocketBase));
    }
}
