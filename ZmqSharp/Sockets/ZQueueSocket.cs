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

    /// <summary>Peer receive-queue lifecycle (0006 3.6): Active while parsing,
    /// Draining while reclaimed on disconnect, Closed afterwards.</summary>
    private enum PeerPhase
    {
        Active,
        Draining,
        Closed,
    }

    private sealed class PeerState
    {
        public required Channel<ZMessage> Queue { get; init; }

        /// <summary>
        /// Serializes queue access between the single reader and the reclaim
        /// drain (SingleReader is a promise, not an enforced exclusion).
        /// </summary>
        public Lock ReadLock { get; } = new();

        public int FrameIndex { get; set; }

        public long AccumulatedLength { get; set; }

        public PeerPhase Phase { get; set; }
    }

    /// <summary>Reads from every peer queue; the peer queues are the only physical queues.</summary>
    private sealed class AggregateReader(ZQueueSocket<TSocket> owner) : ChannelReader<ZMessage>
    {
        public override Task Completion => owner.completion.Task;

        public override bool TryRead(out ZMessage item)
        {
            // Copy-on-write snapshot: a single volatile load, no per-read
            // allocation (0006 3.6). Reclaim drains under the peer's ReadLock,
            // so a message never leaks out of a concurrent drain.
            foreach (var state in owner.peerSnapshot)
            {
                lock (state.ReadLock)
                {
                    if (!state.Queue.Reader.TryRead(out var candidate))
                    {
                        continue;
                    }

                    item = candidate;
                    return true;
                }
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

    /// <summary>
    /// Copy-on-write active-peer snapshot for the aggregate reader: rebuilt
    /// on peer add/remove, read as a single volatile load per read attempt,
    /// so the receive hot path allocates nothing (0006 3.6).
    /// </summary>
    private volatile PeerState[] peerSnapshot = [];
    private readonly ZQueueFactory receiveQueueFactory;
    private readonly ZQueueFactory? sendQueueFactory;
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
        receiveQueueFactory = options.ReceiveQueueFactory;
        ArgumentNullException.ThrowIfNull(receiveQueueFactory);
        sendQueueFactory = options.SendQueueFactory;
        receivePolicy = options.ReceivePolicy;
        maxFrameLength = options.MaxFrameLength;
        maxMessageLength = options.MaxMessageLength;
        maxFramesPerMessage = options.MaxFramesPerMessage;
        Messages = new AggregateReader(this);

        if (sendQueueFactory is { } outboundFactory)
        {
            sendChannel = outboundFactory.Create(static message => message.Dispose());
            sendPump = SendPumpAsync(Cts.Token);
        }

        socket.BindMessageSink(new QueueSurface(this));
        socket.SetFrameAllocator(CreateAllocator);
        socket.SetPeerConnectedHandler(OnPeerConnected);
        socket.PeerEnded += OnPeerEnded;
    }

    public ChannelReader<ZMessage> Messages { get; }

    /// <summary>Total frames rejected by the receive policy since construction.</summary>
    public long ReceiveRejections => Volatile.Read(ref rejections);

    /// <summary>
    /// Optional outbound channel built by <c>SendQueueFactory</c>. When
    /// the send pump fails, the channel completes with that failure so
    /// producers discover it through a failing <c>WriteAsync</c> instead of
    /// waiting for socket disposal (0006 3.5).
    /// </summary>
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
            catch (Exception failure)
            {
                // The pump's failure is surfaced to producers through the
                // outbound channel's completion (0006 3.5); disposal must not
                // fault on that propagation. TryComplete is idempotent - when
                // the pump already completed the channel with this failure it
                // is a no-op, and when the failure came from elsewhere it is
                // surfaced here.
                sendChannel?.Writer.TryComplete(failure);
            }
        }

        await AwaitBackgroundAsync();

        // All pumps have exited and no writer can enqueue. The PeerEnded
        // unsubscribe above means OnPeerEnded no longer runs during disposal,
        // so reclaiming here is the only path for messages that lose their
        // consumer on socket disposal (0006 3.5).
        foreach (var state in peerSnapshot)
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

    /// <summary>
    /// The channel surface bound to the semantic seam (0007 section 2.4): the
    /// transport core aggregates complete messages and delivers them here, per
    /// peer and serialized; backpressure is the peer queue's full state.
    /// </summary>
    private sealed class QueueSurface(ZQueueSocket<TSocket> owner) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token)
            => owner.OnPeerMessageAsync(peer, message, token);
    }

    private void OnPeerConnected(IZConnection connection)
    {
        var state = new PeerState
        {
            Queue = receiveQueueFactory.Create(static message => message.Dispose()),
            Phase = PeerPhase.Active,
        };
        lock (StateLock)
        {
            peers.Add(connection, state);
            PublishAdd(state);
        }
    }

    private async ValueTask OnPeerMessageAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        PeerState? state;
        lock (StateLock)
        {
            peers.TryGetValue(peer, out state);
        }

        if (state is null)
        {
            // The peer ended between message aggregation and delivery; its
            // surface has no consumer, so the message must not leak.
            message.Dispose();
            return;
        }

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

    private (object Owner, int Length) Allocate(ZReceiveMode mode, int length)
    {
        if (mode == ZReceiveMode.Owned)
        {
            var buffer = GC.AllocateUninitializedArray<byte>(length);
            return (buffer, length);
        }

        var owner = Pool.Rent(length);
        return (owner, length);
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
                var (owner, _) = Allocate(allocation.Mode, blockLength);
                segments[i] = new ZSegment(owner, 0, blockLength);
                offset += blockLength;
            }

            return new ZFrame(new ZSegments(segments), more);
        }

        var (singleOwner, _) = Allocate(allocation.Mode, length);
        return new ZFrame(new ZSegment(singleOwner, 0, length), more);
    }

    private bool AnyPeerHasItems()
    {
        foreach (var state in peerSnapshot)
        {
            if (state.Queue.Reader.Count > 0)
            {
                return true;
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

            // Removed from the routing snapshot first, so the aggregate reader
            // can never hand out a message that is about to be drained; no
            // double dispose (0006 3.6).
            state.Phase = PeerPhase.Draining;
            PublishRemove(state);
        }

        Reclaim(state);
        state.Phase = PeerPhase.Closed;

        // A peer that ends with a failure while no other peer remains leaves
        // the send pump with nothing to deliver to. Its sends would otherwise
        // drop silently (retired-peer semantics), so the outbound channel is
        // completed with the failure to surface it to producers deterministically
        // - independent of whether the pump routed before or after the peer's
        // teardown (0006 3.5/3.6).
        if (failure is not null && sendChannel is { } outbound && peerSnapshot.Length == 0)
        {
            outbound.Writer.TryComplete(failure);
        }

        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    /// <summary>
    /// Reclaims every message that loses its consumer: the buffered queue
    /// items. Channel completion alone is not reclamation (0006 section 2.2) -
    /// the buffered items are drained through the same Dispose path a drop
    /// mode uses. The peer's ReadLock serializes the drain against a
    /// concurrent consumer read (SingleReader is a promise, not an exclusion).
    /// </summary>
    private static void Reclaim(PeerState state)
    {
        lock (state.ReadLock)
        {
            state.Queue.Writer.TryComplete();
            while (state.Queue.Reader.TryRead(out var message))
            {
                message.Dispose();
            }
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

    /// <summary>
    /// Publishes a peer into the aggregate-reader snapshot; must be called
    /// while holding <see cref="ZAsyncState.StateLock"/>. Copy-on-write: the
    /// read path is a single volatile load (0006 3.6).
    /// </summary>
    private void PublishAdd(PeerState state)
    {
        var updated = new PeerState[peerSnapshot.Length + 1];
        peerSnapshot.CopyTo(updated, 0);
        updated[^1] = state;
        peerSnapshot = updated;
    }

    private void PublishRemove(PeerState state)
    {
        var current = peerSnapshot;
        var index = Array.IndexOf(current, state);
        if (index < 0)
        {
            return;
        }

        var updated = new PeerState[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        peerSnapshot = updated;
    }

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        try
        {
            await foreach (var message in channel.Reader.ReadAllAsync(token))
            {
                try
                {
                    await socket.SendAsync(message, token);
                }
                catch (OperationCanceledException)
                {
                    // Cancellation is not a failure: the producer completed
                    // the channel or the socket is closing; the dequeued
                    // message must still not leak.
                    message.Dispose();
                    throw;
                }
                catch (Exception failure)
                {
                    // The send consumer is gone (peer failure, protocol error,
                    // closed socket). Reclaim the dequeued message and complete
                    // the producer surface with the failure so producers learn
                    // immediately instead of at socket disposal (0006 3.5).
                    message.Dispose();
                    channel.Writer.TryComplete(failure);
                    return;
                }
            }
        }
        catch (ChannelClosedException)
        {
            // The producer completed the channel first (with or without a
            // failure); the pump exits cleanly.
        }
    }
}
