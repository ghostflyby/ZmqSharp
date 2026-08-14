using System.Buffers;
using System.Threading.Channels;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// Base for every concrete socket that composes the queue receive surface by
/// default (0023): each peer's messages land in that peer's own bounded queue
/// and are read through <see cref="Messages"/> (0004). The queue machinery
/// lived in the retired <c>ZQueueSocket&lt;TSocket&gt;</c> wrapper; it is now
/// owned by the socket itself. <see cref="ZReceiveSurface.Callback"/> opts
/// out: no queue is composed and the raw <c>OnFrame</c> /
/// <see cref="ZSocketBase.BindMessageSink"/> surface is the delivery path.
/// REQ and REP do not derive from this type - their protocol cores consume
/// inbound messages regardless of surface.
/// </summary>
public abstract class ZQueueSocketBase : ZSocketBase
{
    /// <summary>Peer receive-queue lifecycle (0006 3.6): Active while parsing,
    /// Draining while reclaimed on disconnect, Closed afterward.</summary>
    private enum PeerPhase
    {
        Active,
        Draining,
        Closed
    }

    private sealed class PeerState
    {
        public required Channel<ZMessage> Queue { get; init; }

        /// <summary>
        /// Serializes queue access between the single reader and the reclaim
        /// drain (SingleReader is a promise, not an enforced exclusion).
        /// </summary>
        public Lock ReadLock { get; } = new();

        public PeerPhase Phase { get; set; }
    }

    /// <summary>Reads from every peer queue; the peer queues are the only physical queues.</summary>
    private sealed class AggregateReader(ZQueueSocketBase owner) : ChannelReader<ZMessage>
    {
        public override Task Completion => owner.completion.Task;

        public override bool TryRead(out ZMessage item)
        {
            // Copy-on-write snapshot: a single volatile load, no per-read
            // allocation (0006 3.6). Reclaim drains under the peer's ReadLock,
            // so a message never leaks out of a concurrent drain.
            foreach (var state in owner.peerSnapshot)
                lock (state.ReadLock)
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
            if (completed.IsCompleted) return false;

            // Capture the gate before the level check: an item arriving
            // between the check and the await completes the captured gate, so
            // no wake is lost; the 0->1 edge wake means a continuously
            // readable queue does not spin (0006 3.5).
            var wake = owner.GetWakeTask();
            if (owner.AnyPeerHasItems()) return true;

            var done = await Task.WhenAny(wake, completed).WaitAsync(cancellationToken);
            return done == wake;
        }
    }

    private readonly bool queueSurface;
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
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource wakeGate = CreateGate();
    private readonly ChannelReader<ZMessage>? messages;

    /// <summary>
    /// The socket composition face, same shape as <see cref="ZSocketBase"/>
    /// (0019 section 2). When <see cref="ZSocketOptions.ReceiveSurface"/> is
    /// <see cref="ZReceiveSurface.Queue"/> (the default), the queue surface is
    /// bound at construction: per-peer queues, an optional outbound channel,
    /// and per-peer message delivery through <see cref="Messages"/>.
    /// </summary>
    protected ZQueueSocketBase(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null)
        : base(options, dispatch, type, inbound)
    {
        queueSurface = options.ReceiveSurface == ZReceiveSurface.Queue;
        receiveQueueFactory = options.ReceiveQueueFactory;
        ArgumentNullException.ThrowIfNull(receiveQueueFactory);

        if (queueSurface)
        {
            messages = new AggregateReader(this);
            var sendQueueFactory = options.SendQueueFactory;
            if (sendQueueFactory is { } outboundFactory)
            {
                sendChannel = outboundFactory.Create(static message => message.Dispose());
                sendPump = SendPumpAsync(Cts.Token);
            }

            SetReceiveMaterialization(
                options.ReceivePolicy,
                options.MaxFrameLength,
                options.MaxMessageLength,
                options.MaxFramesPerMessage);
            BindMessageSink(new QueueSurface(this));
            SetPeerConnectedHandler(OnPeerConnected);
            PeerEnded += OnPeerEnded;
        }
        else
        {
            messages = null;
        }
    }

    /// <summary>
    /// The aggregate reader over every peer's queue. On the callback surface
    /// (<see cref="ZReceiveSurface.Callback"/>) the socket composes no queue,
    /// so accessing this throws.
    /// </summary>
    public ChannelReader<ZMessage> Messages => messages
        ?? throw new InvalidOperationException("callback surface: the socket composes no queue (set ReceiveSurface = ZReceiveSurface.Queue)");

    /// <summary>Total frames rejected by the receive materialization since construction.</summary>
    public long ReceiveRejections => ReceiveRejectionsCount;

    /// <summary>
    /// Optional outbound channel built by <c>SendQueueFactory</c>. When
    /// the send pump fails, the channel completes with that failure so
    /// producers discover it through a failing <c>WriteAsync</c> instead of
    /// waiting for socket disposal (0006 3.5). Null on the callback surface.
    /// </summary>
    public ChannelWriter<ZMessage>? Outbound => sendChannel?.Writer;

    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0) return;

        PeerEnded -= OnPeerEnded;
        sendChannel?.Writer.TryComplete();

        await StopCoreAsync();

        if (sendPump is not null)
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

        await AwaitBackgroundAsync();

        // All pumps have exited and no writer can enqueue. The PeerEnded
        // unsubscribe above means OnPeerEnded no longer runs during disposal,
        // so reclaiming here is the only path for messages that lose their
        // consumer on socket disposal (0006 3.5).
        foreach (var state in peerSnapshot) Reclaim(state);

        if (sendChannel is { } outbound)
            while (outbound.Reader.TryRead(out var message))
                message.Dispose();

        completion.TrySetResult();
    }

    /// <summary>
    /// The channel surface bound to the semantic seam (0007 section 2.4): the
    /// transport core aggregates complete messages and delivers them here, per
    /// peer and serialized; backpressure is the peer queue's full state.
    /// </summary>
    private sealed class QueueSurface(ZQueueSocketBase owner) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            return owner.OnPeerMessageAsync(peer, message, token);
        }
    }

    private void OnPeerConnected(IZConnection connection)
    {
        var state = new PeerState
        {
            Queue = receiveQueueFactory.Create(static message => message.Dispose()),
            Phase = PeerPhase.Active
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
        if (state.Queue.Reader.Count == 1) Wake();
    }

    private bool AnyPeerHasItems()
    {
        foreach (var state in peerSnapshot)
            if (state.Queue.Reader.Count > 0)
                return true;

        return false;
    }

    private void OnPeerEnded(IZConnection connection, Exception? failure)
    {
        PeerState? state;
        lock (StateLock)
        {
            if (!peers.Remove(connection, out state)) return;

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
            outbound.Writer.TryComplete(failure);
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
            while (state.Queue.Reader.TryRead(out var message)) message.Dispose();
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
        if (index < 0) return;

        var updated = new PeerState[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        peerSnapshot = updated;
    }

    private static TaskCompletionSource CreateGate()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        try
        {
            await foreach (var message in channel.Reader.ReadAllAsync(token))
                try
                {
                    await SendAsync(message, token);
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
        catch (ChannelClosedException)
        {
            // The producer completed the channel first (with or without a
            // failure); the pump exits cleanly.
        }
    }
}
