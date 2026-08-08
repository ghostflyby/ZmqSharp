using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// High-level queue surface: wraps a callback socket type, takes over its
/// per-peer frame delivery at construction, materializes each peer's messages
/// into that peer's own bounded queue (0004), and exposes an aggregate reader
/// over the peer queues. The wrapped socket is never exposed.
/// </summary>
public sealed class ZQueueSocket<TSocket> : ZAsyncState, IZSocket
    where TSocket : ZSocketBase
{
    private sealed class PeerState
    {
        public required Channel<IZMessage> Queue { get; init; }

        public List<ZBufferRef> Accumulator { get; } = [];
    }

    /// <summary>Reads from every peer queue; the peer queues are the only physical queues.</summary>
    private sealed class AggregateReader(ZQueueSocket<TSocket> owner) : ChannelReader<IZMessage>
    {
        public override Task Completion => owner.completion.Task;

        public override bool TryRead([MaybeNullWhen(false)] out IZMessage item)
        {
            foreach (var state in owner.SnapshotPeers())
            {
                if (!state.Queue.Reader.TryRead(out var candidate)) continue;
                item = candidate;
                return true;
            }

            item = null;
            return false;
        }

        public override async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        {
            var completed = owner.completion.Task;
            if (completed.IsCompleted)
            {
                return false;
            }

            var wake = owner.GetWakeTask();
            var done = await Task.WhenAny(wake, completed).WaitAsync(cancellationToken);
            return done == wake;
        }
    }

    private readonly TSocket socket;
    private readonly Channel<IZMessage>? sendChannel;
    private readonly Task? sendPump;
    private readonly Dictionary<IZConnection, PeerState> peers = [];
    private readonly int receiveCapacity;
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Action<IZConnection, Exception?>? peerEnded;
    private TaskCompletionSource wakeGate = CreateGate();

    internal ZQueueSocket(TSocket socket, ZQueueSocketOptions? options = null)
        : base(options?.Pool ?? MemoryPool<byte>.Shared)
    {
        ArgumentNullException.ThrowIfNull(socket);
        this.socket = socket;
        options ??= new ZQueueSocketOptions();
        receiveCapacity = options.ReceiveCapacity;
        Messages = new AggregateReader(this);

        if (options.SendCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<IZMessage>(new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(Cts.Token);
        }

        socket.SetFrameSink(OnPeerConnected);
        socket.PeerEnded += OnPeerEnded;
    }

    public ChannelReader<IZMessage> Messages { get; }

    public ChannelWriter<IZMessage>? Outbound => sendChannel?.Writer;

    /// <summary>Raised when a peer connection ends; null = clean EOF, otherwise the failure.</summary>
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

    public ValueTask SendAsync(IZMessage message, CancellationToken token = default)
        => socket.SendAsync(message, token);

    public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => socket.SendAsync(bytes, token);

    public Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.BindAsync<TEndpoint, TTransport>(endpoint, token);

    public Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.ConnectAsync<TEndpoint, TTransport>(endpoint, token);

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.UnbindAsync<TEndpoint, TTransport>(endpoint);

    public Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.DisconnectAsync<TEndpoint, TTransport>(endpoint);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0)
        {
            return;
        }

        socket.PeerEnded -= OnPeerEnded;
        Cts.Cancel();
        sendChannel?.Writer.TryComplete();

        await socket.DisposeAsync();

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

        completion.TrySetResult();
        await AwaitBackgroundAsync();
        Cts.Dispose();
    }

    private ZFrameHandler OnPeerConnected(IZConnection connection)
    {
        var state = new PeerState
        {
            Queue = Channel.CreateBounded<IZMessage>(
                new BoundedChannelOptions(receiveCapacity) { SingleReader = true }),
        };
        lock (StateLock)
        {
            peers.Add(connection, state);
        }

        TrackBackground(WakeOnReadableAsync(state));
        return (frame, ct) => OnPeerFrame(connection, state, frame, ct);
    }

    private bool OnPeerFrame(IZConnection connection, PeerState state, ZFrame frame, CancellationToken token)
    {
        var owner = Pool.Rent(frame.Memory.Length);
        frame.Memory.CopyTo(owner.Memory);
        state.Accumulator.Add(new ZBufferRef(owner, owner.Memory[..frame.Memory.Length]));

        if (frame.More)
        {
            return true;
        }

        IZMessage message = state.Accumulator.Count == 1
            ? new ZMessage(state.Accumulator[0])
            : new ZMultiMessage([.. state.Accumulator]);
        state.Accumulator.Clear();

        if (state.Queue.Writer.TryWrite(message))
        {
            return true;
        }

        message.Dispose();
        TrackBackground(ResumePeerAsync(state));
        return false;
    }

    private async Task ResumePeerAsync(PeerState state)
    {
        await state.Queue.Writer.WaitToWriteAsync(Cts.Token);
        socket.ResumePaused();
    }

    private async Task WakeOnReadableAsync(PeerState state)
    {
        while (await state.Queue.Reader.WaitToReadAsync(Cts.Token))
        {
            Wake();
        }
    }

    private void OnPeerEnded(IZConnection connection, Exception? failure)
    {
        PeerState? state;
        lock (StateLock)
        {
            if (!peers.Remove(connection, out state))
            {
                return;
            }
        }

        foreach (var frame in state.Accumulator)
        {
            frame.Release();
        }

        state.Accumulator.Clear();
        state.Queue.Writer.TryComplete();

        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    private Task GetWakeTask()
    {
        lock (StateLock)
        {
            return wakeGate.Task;
        }
    }

    private void Wake()
    {
        lock (StateLock)
        {
            wakeGate.TrySetResult();
            wakeGate = CreateGate();
        }
    }

    private List<PeerState> SnapshotPeers()
    {
        lock (StateLock)
        {
            return [.. peers.Values];
        }
    }

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        await foreach (var message in channel.Reader.ReadAllAsync(token))
        {
            await socket.SendAsync(message, token);
        }
    }
}
