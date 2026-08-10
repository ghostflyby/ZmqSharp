using System.Buffers;
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
    private const int SegmentBlockSize = 8192;

    private sealed class PeerState
    {
        public required Channel<ZMessage> Queue { get; init; }

        public List<ZFrame> Accumulator { get; } = [];

        public int FrameIndex { get; set; }

        public long AccumulatedLength { get; set; }
    }

    /// <summary>Reads from every peer queue; the peer queues are the only physical queues.</summary>
    private sealed class AggregateReader(ZQueueSocket<TSocket> owner) : ChannelReader<ZMessage>
    {
        public override Task Completion => owner.completion.Task;

        public override bool TryRead(out ZMessage item)
        {
            foreach (var state in owner.SnapshotPeers())
            {
                if (!state.Queue.Reader.TryRead(out var candidate)) continue;
                item = candidate;
                return true;
            }

            item = default;
            return false;
        }

        public override async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        {
            var completed = owner.completion.Task;
            if (completed.IsCompleted)
            {
                return false;
            }

            // Capture the gate before the level check: an item arriving
            // between the check and the await completes the captured gate, so
            // no wake is lost; the 0->1 edge wake means a continuously
            // readable queue does not spin (0006 3.5).
            var wake = owner.GetWakeTask();
            if (owner.AnyPeerHasItems())
            {
                return true;
            }

            var done = await Task.WhenAny(wake, completed).WaitAsync(cancellationToken);
            return done == wake;
        }
    }

    private readonly TSocket socket;
    private readonly Channel<ZMessage>? sendChannel;
    private readonly Task? sendPump;
    private readonly Dictionary<IZConnection, PeerState> peers = [];
    private readonly int receiveCapacity;
    private readonly BoundedChannelFullMode receiveFullMode;
    private readonly IZReceivePolicy receivePolicy;
    private readonly long maxFrameLength;
    private readonly long maxMessageLength;
    private readonly int maxFramesPerMessage;
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long rejections;
    private Action<IZConnection, Exception?>? peerEnded;
    private TaskCompletionSource wakeGate = CreateGate();

    internal ZQueueSocket(TSocket socket, ZQueueSocketOptions? options = null)
        : base(options?.Pool ?? MemoryPool<byte>.Shared)
    {
        ArgumentNullException.ThrowIfNull(socket);
        this.socket = socket;
        options ??= new ZQueueSocketOptions();
        receiveCapacity = options.ReceiveCapacity;
        receiveFullMode = ToBoundedFullMode(options.ReceiveFullMode);
        receivePolicy = options.ReceivePolicy;
        maxFrameLength = options.MaxFrameLength;
        maxMessageLength = options.MaxMessageLength;
        maxFramesPerMessage = options.MaxFramesPerMessage;
        Messages = new AggregateReader(this);

        if (options.SendCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<ZMessage>(new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(Cts.Token);
        }

        socket.SetFrameSink(OnPeerConnected);
        socket.SetFrameAllocator(CreateAllocator);
        socket.PeerEnded += OnPeerEnded;
    }

    public ChannelReader<ZMessage> Messages { get; }

    /// <summary>Total frames rejected by the receive policy since construction.</summary>
    public long ReceiveRejections => Volatile.Read(ref rejections);

    public ChannelWriter<ZMessage>? Outbound => sendChannel?.Writer;

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

    public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
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
        await Cts.CancelAsync();
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
            catch (ObjectDisposedException)
            {
                // The pump may have dequeued a message just before the socket
                // closed and failed its send on the closed socket; the pump
                // disposed it before rethrowing.
            }
        }

        await AwaitBackgroundAsync();

        // All pumps have exited and no writer can enqueue. The PeerEnded
        // unsubscribe above means OnPeerEnded no longer runs during disposal,
        // so reclaiming here is the only path for messages that lose their
        // consumer on socket disposal (0006 3.5).
        foreach (var state in SnapshotPeers())
        {
            Reclaim(state);
        }

        if (sendChannel is { } outbound)
        {
            while (outbound.Reader.TryRead(out var message))
            {
                message.Dispose();
            }
        }

        completion.TrySetResult();
        Cts.Dispose();
    }

    private ZFrameHandlerAsync OnPeerConnected(IZConnection connection)
    {
        var state = new PeerState
        {
            Queue = Channel.CreateBounded<ZMessage>(
                new BoundedChannelOptions(receiveCapacity)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = receiveFullMode,
                },
                // Every message dropped by a drop mode is reclaimed here; a
                // consumer can never observe it (0006 section 2.2).
                static message => message.Dispose()),
        };
        lock (StateLock)
        {
            peers.Add(connection, state);
        }

        return (frame, ct) => OnPeerFrameAsync(state, frame, ct);
    }

    private async ValueTask<bool> OnPeerFrameAsync(PeerState state, ZFrame frame, CancellationToken token)
    {
        if (frame.TryGetValue(out ZSegments segments))
        {
            state.Accumulator.Add(new ZFrame(segments, frame.More));
        }
        else if (frame.TryGetValue(out ZSegment segment) && !segment.IsBorrowed)
        {
            state.Accumulator.Add(new ZFrame(segment, frame.More));
        }
        else
        {
            frame.TryGetValue(out ZSegment borrowed);
            var poolOwner = Pool.Rent(borrowed.Memory.Length);
            borrowed.Memory.CopyTo(poolOwner.Memory);
            state.Accumulator.Add(new ZFrame(
                new ZSegment(poolOwner, poolOwner.Memory[..borrowed.Memory.Length]),
                frame.More));
        }

        if (frame.More)
        {
            return true;
        }

        var message = BuildMessage(state.Accumulator);
        state.Accumulator.Clear();
        state.FrameIndex = 0;
        state.AccumulatedLength = 0;

        // Wait-mode backpressure: a full per-peer queue pauses only this
        // peer's pump on the WriteAsync; the parser awaits the sink, so no
        // global resume coordination is needed (0006 section 3.5).
        try
        {
            await state.Queue.Writer.WriteAsync(message, token);
        }
        catch
        {
            // Cancellation or a closed queue loses this message's consumer;
            // the buffer must not leak.
            message.Dispose();
            throw;
        }

        // Edge wake: a single writer means Count == 1 is exactly the 0->1
        // transition, so waiting readers wake once per message batch instead
        // of spinning on a continuously readable queue. A drop-mode write to
        // a full queue leaves Count unchanged and wakes nobody (0006 3.5).
        if (state.Queue.Reader.Count == 1)
        {
            Wake();
        }

        return true;
    }

    private static ZMessage BuildMessage(List<ZFrame> frames)
    {
        if (frames.Count == 1)
        {
            return new ZMessage(new ZSingleMessage(frames[0]));
        }

        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    private ZFrameAllocator CreateAllocator(IZConnection connection)
    {
        return (length, more) =>
        {
            PeerState state;
            lock (StateLock)
            {
                if (!peers.TryGetValue(connection, out var s))
                {
                    throw new InvalidOperationException("frame allocator invoked for an unknown peer");
                }

                state = s;
            }

            var frameIndex = state.FrameIndex;
            if (!ZReceiveGuard.TryAccumulate(state.AccumulatedLength, length, out var accumulated))
            {
                // An unrepresentable message total is a rejection, never an
                // arithmetic exception (0008 D3/D6).
                Reject(ZReceiveRejectionReason.MessageTooLarge, null, null);
            }

            state.FrameIndex = frameIndex + 1;
            state.AccumulatedLength = accumulated;

            // Connection-level limits are enforced here, before any allocation,
            // so a custom policy can never bypass them (0008 D1).
            if (ZReceiveGuard.CheckLimits(
                    length,
                    accumulated,
                    frameIndex,
                    maxFrameLength,
                    maxMessageLength,
                    maxFramesPerMessage) is { } rejection)
            {
                Reject(rejection);
            }

            var allocation = DecideAllocation(frameIndex, accumulated, length, more);
            return AllocateSegments(allocation, length, more);
        };
    }

    private ZReceiveAllocation DecideAllocation(
        int frameIndex,
        long accumulatedLength,
        int frameLength,
        bool more)
        => receivePolicy.Decide(new ZReceiveContext
        {
            FrameLength = frameLength,
            HasMore = more,
            FrameIndex = frameIndex,
            AccumulatedLength = accumulatedLength,
        });

    private void Reject(ZReceiveRejection rejection)
    {
        Interlocked.Increment(ref rejections);
        throw new ZReceiveRejectedException(rejection);
    }

    private void Reject(ZReceiveRejectionReason reason, long? limit, long? actual)
        => Reject(new ZReceiveRejection { Reason = reason, Limit = limit, Actual = actual });

    private (object Owner, Memory<byte> Memory) Allocate(ZReceiveMode mode, int length)
    {
        if (mode == ZReceiveMode.Owned)
        {
            var buffer = GC.AllocateUninitializedArray<byte>(length);
            return (buffer, buffer);
        }

        var owner = Pool.Rent(length);
        return (owner, owner.Memory[..length]);
    }

    private ZFrame AllocateSegments(
        ZReceiveAllocation allocation,
        int length,
        bool more)
    {
        if (allocation.Segmented && length > SegmentBlockSize)
        {
            var count = (length + SegmentBlockSize - 1) / SegmentBlockSize;
            var segments = new ZSegment[count];
            var offset = 0;
            for (var i = 0; i < count; i++)
            {
                var blockLength = Math.Min(SegmentBlockSize, length - offset);
                var (owner, memory) = Allocate(allocation.Mode, blockLength);
                segments[i] = new ZSegment(owner, memory);
                offset += blockLength;
            }

            return new ZFrame(new ZSegments(segments), more);
        }

        var (singleOwner, singleMemory) = Allocate(allocation.Mode, length);
        return new ZFrame(new ZSegment(singleOwner, singleMemory), more);
    }

    private bool AnyPeerHasItems()
    {
        lock (StateLock)
        {
            foreach (var state in peers.Values)
            {
                if (state.Queue.Reader.Count > 0)
                {
                    return true;
                }
            }
        }

        return false;
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

        // Removed from the dictionary first, so the aggregate reader can never
        // hand out a message that is about to be drained; no double dispose.
        Reclaim(state);

        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    /// <summary>
    /// Reclaims every message that loses its consumer: the owning frames of an
    /// incomplete message plus the buffered queue items. Channel completion
    /// alone is not reclamation (0006 section 2.2) - the buffered items are
    /// drained through the same Dispose path a drop mode uses.
    /// </summary>
    private static void Reclaim(PeerState state)
    {
        foreach (var frame in state.Accumulator)
        {
            frame.Dispose();
        }

        state.Accumulator.Clear();
        state.Queue.Writer.TryComplete();
        while (state.Queue.Reader.TryRead(out var message))
        {
            message.Dispose();
        }
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
            try
            {
                await socket.SendAsync(message, token);
            }
            catch
            {
                // The pump's consumer is gone (the socket failed or closed);
                // the dequeued message must not leak.
                message.Dispose();
                throw;
            }
        }
    }

    internal static BoundedChannelFullMode ToBoundedFullMode(ZQueueFullMode mode) => mode switch
    {
        ZQueueFullMode.Wait => BoundedChannelFullMode.Wait,
        ZQueueFullMode.DropWrite => BoundedChannelFullMode.DropWrite,
        ZQueueFullMode.DropNewest => BoundedChannelFullMode.DropNewest,
        ZQueueFullMode.DropOldest => BoundedChannelFullMode.DropOldest,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "unknown receive full mode"),
    };
}
