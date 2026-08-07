using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared socket mechanics: calls the transport, stores connections, drives the
/// handshake externally, and orchestrates policy dispatch and backpressure.
/// </summary>
internal sealed class ZSocketBase : IZSocket
{
    private static readonly byte[] ReadyCommand = [.. "READY\0"u8];

    private readonly record struct Peer(IZConnection Connection, string Endpoint, ZmtpParser Parser);

    private readonly IZSchedulingPolicy policy;
    private readonly MemoryPool<byte> pool;
    private readonly Lock stateLock = new();
    private readonly List<Peer> peers = [];
    private readonly List<(IZTransport Listener, string Endpoint)> listeners = [];
    private readonly List<Task> backgroundTasks = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];
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

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, string.Empty);
        return ValueTask.CompletedTask;
    }

    private void AddConnection(IZConnection connection, string endpoint)
    {
        var parser = new ZmtpParser(connection, pool);
        var accumulator = new List<ZBufferRef>();
        connection.SetFrameHandler((frame, ct) => DeliverFrame(parser, frame, accumulator));
        connection.SetConnectionEndedHandler(() => Release(accumulator));

        lock (stateLock)
        {
            ThrowIfClosed();
            peers.Add(new Peer(connection, endpoint, parser));
        }

        TrackBackground(RunConnectionAsync(connection, parser));
    }

    private async Task RunConnectionAsync(IZConnection connection, ZmtpParser parser)
    {
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
            Fail(ex);
        }
        finally
        {
            connection.OnConnectionEnded();
            lock (stateLock)
            {
                peers.RemoveAll(peer => peer.Connection == connection);
            }
        }

        parser.Dispose();
        connection.Dispose();
    }

    private bool DeliverFrame(ZmtpParser parser, ZFrame frame, List<ZBufferRef> accumulator)
    {
        var keepGoing = RaiseOnFrame(frame);
        if (receiveChannel is not null)
        {
            keepGoing &= Accumulate(parser, frame, accumulator);
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

    private bool Accumulate(ZmtpParser parser, ZFrame frame, List<ZBufferRef> accumulator)
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
        paused.Enqueue(parser);
        TrackBackground(ResumePausedAsync());
        return false;
    }

    private void Release(List<ZBufferRef> accumulator)
    {
        foreach (var frame in accumulator)
        {
            frame.Release();
        }

        accumulator.Clear();
    }

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        await foreach (var message in channel.Reader.ReadAllAsync(token))
        {
            await SendAsync(message, token);
        }
    }

    private async Task ResumePausedAsync()
    {
        var channel = receiveChannel;
        if (channel is null)
        {
            return;
        }

        while (paused.TryDequeue(out var parser))
        {
            await channel.Writer.WaitToWriteAsync(cts.Token);
            parser.Resume();
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
        }
        catch (ZeroMqProtocolException)
        {
        }
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

        cts.Cancel();
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

    private List<IZConnection> SnapshotPeers()
    {
        lock (stateLock)
        {
            return [.. peers.Select(peer => peer.Connection)];
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
