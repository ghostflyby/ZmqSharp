using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Sockets;
using ZmqSharp.Patterns;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

// ReSharper disable once CheckNamespace
namespace ZmqSharp;

/// <summary>
/// Pattern-agnostic transport core (0007 section 2.1): calls the transport,
/// stores connections, drives the handshake externally, aggregates messages
/// for a bound <see cref="IPatternSink"/>, and delivers borrowed frames to the
/// raw OnFrame callback. Behavior is composed as three independent seams
/// (0015 section 2.1 / 0019): outbound selection (<see cref="IZDispatchPolicy"/>),
/// inbound processing (<see cref="IZInboundPolicy"/>), and the advertised
/// socket identity (<see cref="ZSocketType"/>); socket types are thin
/// composition roots over the protected constructor.
/// </summary>
public abstract class ZSocketBase : ZAsyncState, IZSocket
{
    /// <summary>
    /// Copy-on-write routable-peer snapshot: rebuilt only when a peer is
    /// added or removed, read as a single volatile load on the hot path, so
    /// a send never allocates a peer list (0006 3.6).
    /// </summary>
    private volatile IZConnection[] peerSnapshot = [];

    /// <summary>
    /// Per-peer session connection: after the mechanism handshake, sends route
    /// through the session connection the mechanism returned (0016 section 9) -
    /// a CURVE session wraps the raw connection to encrypt on write. The peer
    /// identity for routing, gates, and PeerEnded stays the raw connection.
    /// </summary>
    private readonly Dictionary<IZConnection, IZConnection> sessionConnections = [];

    /// <summary>Per-connection endpoint, used only by the rare DisconnectAsync lookup.</summary>
    private readonly Dictionary<IZConnection, object?> endpoints = [];

    private readonly List<(IZTransport Listener, object? Endpoint)> listeners = [];
    private readonly Dictionary<IZConnection, TaskCompletionSource> establishedGates = [];
    private readonly Dictionary<IZConnection, CancellationTokenSource> attemptTokens = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];

    /// <summary>Per-peer frame accumulators for message aggregation (0007 2.3).</summary>
    private readonly Dictionary<IZConnection, PeerAccumulator> accumulators = [];

    private readonly int maxCommandSize;
    private readonly IZDispatchPolicy dispatch;
    private readonly ZSocketType type;
    private readonly IZInboundPolicy inbound;
    private readonly IZSecurityMechanism mechanism;
    private readonly ReadOnlyMemory<byte> localReadyBody;
    private readonly int handshakeTimeoutMs;
    private readonly int maxIncompleteHandshakes;
    private int incompleteHandshakes;
    private ZFrameHandler? onFrame;
    private IPatternSink? messageSink;
    private Action<IZConnection>? peerConnected;
    private Action<IZConnection, Exception?>? peerEnded;

    // Receive materialization (0007 2.1): allocation policy and the 0008 guard
    // limits, applied per connection by a ReceiveMaterializer.
    private IZReceivePolicy? receivePolicy;
    private long maxFrameLength = long.MaxValue;
    private long maxMessageLength = long.MaxValue;
    private int maxFramesPerMessage = int.MaxValue;
    private long receiveRejections;

    /// <summary>
    /// Raised when the receive materializer rejects a frame (an over-limit
    /// frame, a policy rejection, or an empty message). Test seam: lets tests
    /// wait on the rejection state directly instead of polling the counter.
    /// </summary>
    internal event Action? MaterializerRejected;

    /// <summary>
    /// The socket composition face (0019 section 2): outbound dispatch, socket
    /// identity, and inbound processing. <paramref name="inbound"/> defaults to
    /// pass-through delivery, so sockets that only route outbound (single-peer,
    /// round-robin, broadcast) declare nothing inbound. Queue configuration is
    /// rejected here when the socket never composes a queue
    /// (see <see cref="ComposesQueueSurface"/>).
    /// </summary>
    protected ZSocketBase(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null)
        : base(options.Pool)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dispatch);
        ArgumentNullException.ThrowIfNull(type);
        this.dispatch = dispatch;
        this.type = type;
        this.inbound = inbound ?? ZInboundPolicy.PassThrough;
        maxCommandSize = options.MaxCommandSize;
        mechanism = options.Security.Mechanism;
        handshakeTimeoutMs = options.HandshakeTimeoutMs;
        maxIncompleteHandshakes = options.MaxIncompleteHandshakes;
        messageSink = options.MessageSink;
        if (!ComposesQueueSurface && options.HasQueueConfiguration)
            throw new InvalidOperationException(
                "queue configuration (ReceiveQueueFactory/ReceivePolicy/limits) requires the queue surface; this socket never composes a queue");
        // The local READY body depends only on the socket type; building it
        // once per socket instead of once per connection keeps the handshake
        // cold path allocation-free (0023 D6).
        localReadyBody = ZmtpCommands.BuildReady(type.Name);
    }

    /// <summary>
    /// True when the socket composes the queue receive surface (0023).
    /// Override in a custom socket type that derives from
    /// <see cref="ZQueueSocketBase"/>; queue options on a socket that returns
    /// false are rejected at construction instead of silently ignored.
    /// </summary>
    protected virtual bool ComposesQueueSurface => false;

    /// <summary>The routable-peer snapshot (dispatch policies read it for outbound selection).</summary>
    internal IZConnection[] PeerSnapshot => peerSnapshot;

    /// <summary>The composed inbound policy (protocol sockets hold it to route consume decisions).</summary>
    internal IZInboundPolicy InboundPolicy => inbound;

    /// <summary>
    /// True when inbound messages must be aggregated into complete messages:
    /// a sink is bound, or the composed inbound policy is not the pass-through
    /// default (protocol sockets such as REQ/REP consume on the aggregated
    /// tier without a public sink). The borrowed frame tier runs otherwise.
    /// </summary>
    private bool NeedsAggregation => messageSink is not null || !ReferenceEquals(inbound, ZInboundPolicy.PassThrough);

    public event ZFrameHandler? OnFrame
    {
        add
        {
            lock (StateLock)
            {
                // Exactly one consumer of the delivery stream (0007 section 1):
                // a message sink and a composed inbound policy are mutually
                // exclusive with the raw frame surface on the same instance.
                // Sockets with a non-default inbound policy (ROUTER, SUB, XPUB,
                // REQ, REP) always aggregate, so their delivery stream is
                // consumed by the policy; subscribing to OnFrame on them fails
                // loudly instead of silently receiving nothing.
                if (messageSink is not null)
                    throw new InvalidOperationException("cannot subscribe to OnFrame after a message sink is bound");

                if (!ReferenceEquals(inbound, ZInboundPolicy.PassThrough))
                    throw new InvalidOperationException(
                        "cannot subscribe to OnFrame on a socket with a composed inbound policy; bind a message sink");

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
        while (paused.TryDequeue(out var parser)) parser.Resume();
    }

    /// <summary>
    /// Registers a per-peer connection callback for a composed surface
    /// (creates the surface's per-connection state, e.g. the queue surface's
    /// PeerState or SUB's subscription broadcast); must be called before any
    /// connection is established. Multicast: a socket may compose several
    /// per-peer callbacks (0023 - the queue surface and SUB's subscription
    /// propagation both observe new peers).
    /// </summary>
    internal void SetPeerConnectedHandler(Action<IZConnection> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
                throw new InvalidOperationException(
                    "peer connected handler must be set before connections are established");

            peerConnected += handler;
        }
    }

    /// <summary>
    /// Binds a delivery sink for the composed surface (0023): the queue
    /// surface binds its internal <c>QueueSurface</c> here at construction,
    /// and a custom sink is configured through
    /// <see cref="ZSocketOptions.MessageSink"/> - user code never calls this.
    /// Must be called before any connection is established; a bound sink is
    /// mutually exclusive with the raw <see cref="OnFrame"/> surface.
    /// </summary>
    internal void BindMessageSink(IPatternSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
                throw new InvalidOperationException("message sink must be bound before connections are established");

            messageSink = sink;
        }
    }

    /// <summary>
    /// Configures receive materialization for the transport core (0007 2.1):
    /// the allocation policy and the connection-level limits (0008 D1) applied
    /// per connection by a <see cref="ReceiveMaterializer"/>. Composed by the
    /// queue surface at construction (0023); a callback-only socket keeps the
    /// null policy and runs the borrowed frame tier. Must be called before any
    /// connection is established.
    /// </summary>
    internal void SetReceiveMaterialization(
        IZReceivePolicy policy,
        long maxFrameLength,
        long maxMessageLength,
        int maxFramesPerMessage)
    {
        ArgumentNullException.ThrowIfNull(policy);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
                throw new InvalidOperationException(
                    "receive materialization must be set before connections are established");

            receivePolicy = policy;
            this.maxFrameLength = maxFrameLength;
            this.maxMessageLength = maxMessageLength;
            this.maxFramesPerMessage = maxFramesPerMessage;
        }
    }

    /// <summary>Total frames rejected by the receive materialization since construction.</summary>
    internal long ReceiveRejectionsCount => Volatile.Read(ref receiveRejections);

    private void OnMaterializerRejected()
    {
        Interlocked.Increment(ref receiveRejections);
        MaterializerRejected?.Invoke();
    }

    public async Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var connection = await TTransport.ConnectAsync(endpoint, new ZTransportOptions(), token);
        var established = AddConnection(connection, endpoint, ZMechanismRole.Client, token);
        await established.Task.WaitAsync(token);
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
            matches = [];
            foreach (var connection in peerSnapshot)
                if (endpoints.TryGetValue(connection, out var peerEndpoint) && Equals(peerEndpoint, endpoint))
                    matches.Add(connection);

            foreach (var match in matches)
                // Cancel the in-flight attempt first: disposing the transport
                // stream does not reliably interrupt a pending read, which
                // would leave the connection pump stuck and the peer routable.
                if (attemptTokens.TryGetValue(match, out var attemptCts))
                    attemptCts.Cancel();
        }

        foreach (var match in matches) match.Dispose();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops the socket: cancels the background work, disposes listeners, and
    /// awaits the connection pumps. The queue surface composes its own teardown
    /// (reclaiming buffered messages) around this in <see
    /// cref="ZQueueSocketBase"/>.
    /// </summary>
    public virtual async ValueTask DisposeAsync()
    {
        await StopAsync();
        await AwaitBackgroundAsync();
    }

    private async Task StopAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0) return;

        await StopCoreAsync();
    }

    /// <summary>
    /// Disposes listeners and cancels background work; the Closed flag is
    /// already set when this runs. Subclasses that stop in their own order
    /// (the queue surface completes and drains its outbound channel before
    /// stopping the connection pumps) call this from their teardown.
    /// </summary>
    protected async Task StopCoreAsync()
    {
        await Cts.CancelAsync();
        List<IZTransport> listenerSnapshot;
        lock (StateLock)
        {
            listenerSnapshot = [.. listeners.Select(entry => entry.Listener)];
            listeners.Clear();
        }

        foreach (var listener in listenerSnapshot) listener.Dispose();

        Cts.Dispose();
    }

    /// <summary>
    /// Selective send (0015 section 2.1): the dispatch policy is the sole
    /// decision maker - it selects zero or more targets from the routable
    /// peer set, and the message is sent to exactly those peers, once each.
    /// Caller-addressed sends (ROUTER identity, REP replies) bypass this path
    /// through <see cref="SendToAsync"/>. The message is disposed once after
    /// the loop, in <see cref="SendAsyncCore(ZMessage, CancellationToken)"/>.
    /// Protected: the public send surface is decided by each socket type
    /// (0024), not inherited from the base.
    /// </summary>
    protected async ValueTask SendAsyncCore(ZMessage message, CancellationToken token = default)
    {
        ThrowIfClosed();
        try
        {
            await SendToTargetsAsync(message, token);
        }
        finally
        {
            message.Dispose();
        }
    }

    private async ValueTask SendToTargetsAsync(ZMessage message, CancellationToken token)
    {
        var peers = peerSnapshot;
        // The policy may select up to every peer; the target buffer is rented
        // so the steady-state path stays GC-allocation-free (ArrayPool reuses
        // the array, 0006 3.6).
        var targets = ArrayPool<IZConnection>.Shared.Rent(peers.Length);
        try
        {
            var count = dispatch.SelectTargets(message, peers, targets.AsSpan(0, peers.Length));
            for (var i = 0; i < count; i++) await SendToPeerAsync(targets[i], message, token);
        }
        finally
        {
            ArrayPool<IZConnection>.Shared.Return(targets);
        }
    }

    /// <summary>
    /// Establishes the ZMTP handshake within <c>HandshakeTimeoutMs</c> (0006
    /// 3.2); a timed-out handshake faults the establishment with a
    /// <see cref="TimeoutException"/>. The handshake covers the greeting and
    /// the whole mechanism command sequence, so a peer that stalls mid-handshake
    /// faults exactly as before (0016 section 8).
    /// </summary>
    private async Task<ZMechanismResult?> EstablishWithTimeoutAsync(
        ZmtpHandshake handshake,
        ZMechanismRole role,
        CancellationToken token)
    {
        if (handshakeTimeoutMs > 0)
            return await handshake.EstablishAsync(role, token).AsTask().WaitAsync(
                TimeSpan.FromMilliseconds(handshakeTimeoutMs), token);

        return await handshake.EstablishAsync(role, token);
    }

    /// <summary>
    /// Directed send to a specific connection (0007 section 2.1 primitive,
    /// used by REP reply routing and ROUTER identity addressing). The message
    /// is disposed after the send, exactly once.
    /// </summary>
    internal async ValueTask SendToAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
    {
        try
        {
            ThrowIfClosed();
            await SendToPeerAsync(peer, message, token);
        }
        finally
        {
            // The message is disposed even when the send races socket closure
            // (ThrowIfClosed throws before the send; the caller must never
            // leak it, e.g. XPUB's fire-and-forget subscription forwards).
            message.Dispose();
        }
    }

    private async ValueTask SendToPeerAsync(IZConnection connection, ZMessage message, CancellationToken token)
    {
        await WaitUntilEstablishedAsync(connection, token);

        // Sends go through the mechanism's session connection (the CURVE
        // encrypting wrapper); the peer snapshot keeps the raw connection as
        // the identity (0016 section 9). A read-only lookup with no
        // allocation on the hot path.
        var peer = connection;
        lock (StateLock)
        {
            sessionConnections.TryGetValue(connection, out peer);
        }

        peer ??= connection;
        try
        {
            await peer.SendAsync(message, token);
        }
        catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
        {
            // The peer's connection retired between routing and the write
            // (or mid-write); a send to a dying peer is dropped, never a
            // fault. The retirement is already surfaced through PeerEnded
            // (0006 3.6).
        }
    }

    /// <summary>
    /// Bytes overload of the selective send: pools a copy of the payload and
    /// routes it through <see cref="SendAsyncCore(ZMessage, CancellationToken)"/>.
    /// Protected; the public send surface is decided by each socket type (0024).
    /// </summary>
    protected async ValueTask SendAsyncCore(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ThrowIfClosed();
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, bytes.Length))));
        await SendAsyncCore(message, token);
    }

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, null, ZMechanismRole.Server);
        return ValueTask.CompletedTask;
    }

    private TaskCompletionSource AddConnection(IZConnection connection, object? endpoint,
        ZMechanismRole role = ZMechanismRole.Client, CancellationToken token = default)
    {
        ReceiveMaterializer? materializer = null;
        ZFrameAllocator? allocator = null;
        var established = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (StateLock)
        {
            if (Volatile.Read(ref Closed) == 1)
            {
                // The socket closed while this connection was being set up;
                // report cancellation instead of success so ConnectAsync never
                // completes as established. Accepted peers are dropped silently.
                connection.Dispose();
                established.TrySetCanceled();
                return established;
            }

            if (receivePolicy is { } policy)
            {
                materializer = new ReceiveMaterializer(
                    Pool, policy, maxFrameLength, maxMessageLength, maxFramesPerMessage, OnMaterializerRejected);
                allocator = materializer.CreateAllocator();
            }

            // Inbound (accepted) connections are the uncontrolled surface: cap
            // the number of incomplete handshakes so a slow-connecting flood
            // cannot exhaust resources (0006 3.2). Outbound ConnectAsync is
            // caller-initiated and not gated.
            if (endpoint is null && maxIncompleteHandshakes > 0 && incompleteHandshakes >= maxIncompleteHandshakes)
            {
                connection.Dispose();
                established.TrySetCanceled();
                return established;
            }

            incompleteHandshakes++;
            establishedGates[connection] = established;
            // Register before the pump starts. Sends block on the establishment
            // gate until the handshake completes, and DisconnectAsync must
            // always be able to find the peer to cancel the in-flight attempt.
            PublishAdd(connection, endpoint);
            peerConnected?.Invoke(connection);
        }

        var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(Cts.Token, token);
        lock (StateLock)
        {
            attemptTokens[connection] = attemptCts;
        }

        var pump = RunConnectionAsync(connection, established, role, allocator, materializer, attemptCts);
        TrackBackground(pump);
        return established;
    }

    private async Task RunConnectionAsync(
        IZConnection connection,
        TaskCompletionSource established,
        ZMechanismRole role,
        ZFrameAllocator? allocator,
        ReceiveMaterializer? materializer,
        CancellationTokenSource attemptCts)
    {
        Exception? failure = null;
        var attemptToken = attemptCts.Token;
        ZmtpParser? parser = null;
        try
        {
            // The handshake writes the greeting, matches the mechanism, and
            // runs its command sequence; the socket-type predicate then
            // validates the peer's READY (0015 section 2.4). The parser is
            // traffic-only and starts on the session connection the mechanism
            // returns.
            using var handshake = new ZmtpHandshake(
                connection,
                mechanism,
                localReadyBody,
                maxCommandSize,
                Pool);
            var result = await EstablishWithTimeoutAsync(handshake, role, attemptToken);
            if (result is null)
            {
                var eof = new IOException("peer closed during ZMTP handshake");
                failure = eof;
                established.TrySetException(eof);
                return;
            }

            var peerType = ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span);
            if (!type.AcceptsPeer(peerType))
            {
                // RFC 23: on socket-type validation failure, return an
                // ERROR command before disconnecting the peer.
                await connection.SendCommandAsync(ZmtpCommands.BuildError("Invalid socket type"), attemptToken);
                throw new ZeroMqProtocolException(
                    $"peer socket type '{peerType}' is not accepted by local socket type '{type.Name}'");
            }

            parser = new ZmtpParser(result.Value.SessionConnection, allocator, Pool, maxCommandSize);
            // Aggregation runs when a sink is bound or the composed inbound
            // policy needs complete messages (protocol sockets such as REQ/REP
            // consume on the aggregated tier without a public sink).
            var trafficHandler = NeedsAggregation
                ? MessageSinkHandler(connection, parser, materializer)
                : BorrowedSink(parser);
            connection.SetFrameHandler((frame, _) => trafficHandler(frame, Cts.Token));
            lock (StateLock)
            {
                sessionConnections[connection] = result.Value.SessionConnection;
            }

            established.TrySetResult();
            await parser.ParseAsync(attemptToken);
        }
        catch (OperationCanceledException)
        {
            established.TrySetCanceled(attemptToken);
        }
        catch (ZeroMqProtocolException ex)
        {
            failure = ex;
            established.TrySetException(ex);
        }
        catch (Exception ex) when (ex is IOException or SocketException)
        {
            // An IO failure ends this peer. Cancelling the socket aborts
            // pending reads/writes, which on Windows can surface as a reset
            // instead of OperationCanceledException; that teardown race is
            // not a real failure and is not reported.
            if (!attemptToken.IsCancellationRequested)
            {
                failure = ex;
                established.TrySetException(ex);
            }
            else
            {
                established.TrySetCanceled(attemptToken);
            }
        }
        catch (Exception ex)
        {
            failure = ex;
            established.TrySetException(ex);
        }
        finally
        {
            // Internal state first: a failing callback must not leave the peer routable.
            lock (StateLock)
            {
                PublishRemove(connection);
                establishedGates.Remove(connection);
                attemptTokens.Remove(connection);
                sessionConnections.Remove(connection);
                incompleteHandshakes--;
                if (accumulators.Remove(connection, out var accumulator))
                    // The peer ended mid-message: owning frames accumulated for
                    // the incomplete message have no consumer and must not leak.
                    foreach (var frame in accumulator.Frames)
                        frame.Dispose();
            }

            try
            {
                connection.OnConnectionEnded();
                RaisePeerEnded(connection, failure);
            }
            finally
            {
                parser?.Dispose();
                connection.Dispose();
                attemptCts.Dispose();
            }
        }
    }

    private async ValueTask WaitUntilEstablishedAsync(IZConnection connection, CancellationToken token)
    {
        TaskCompletionSource? gate;
        lock (StateLock)
        {
            establishedGates.TryGetValue(connection, out gate);
        }

        // An established peer's gate completed successfully; skipping the
        // WaitAsync keeps the steady-state send path allocation-free (0006 3.6).
        if (gate is null || gate.Task.IsCompletedSuccessfully) return;

        await gate.Task.WaitAsync(token);
    }

    /// <summary>
    /// Default per-peer sink for the borrowed tier: invokes the raw OnFrame
    /// callback; a false return pauses this peer's pump until ResumePaused.
    /// </summary>
    private ZFrameHandlerAsync BorrowedSink(ZmtpParser parser)
    {
        return (frame, _) =>
        {
            var keepGoing = RaiseOnFrame(frame);
            if (!keepGoing) paused.Enqueue(parser);

            return ValueTask.FromResult(keepGoing);
        };
    }

    private bool RaiseOnFrame(ZFrame frame)
    {
        ZFrameHandler? handler;
        lock (StateLock)
        {
            handler = onFrame;
        }

        if (handler is null) return true;

        var keepGoing = true;
        foreach (var item in handler.GetInvocationList()) keepGoing &= ((ZFrameHandler)item)(frame, Cts.Token);

        return keepGoing;
    }

    /// <summary>
    /// Aggregates a peer's frames into complete messages for the message sink
    /// (0007 section 2.3). A borrowed frame is copied into a pooled buffer
    /// before it is retained, mirroring the queue surface's previous
    /// accumulation path. The receive materializer's guard counters reset at
    /// each message boundary.
    /// </summary>
    private sealed class PeerAccumulator
    {
        public List<ZFrame> Frames { get; } = [];

        public ReceiveMaterializer? Materializer { get; init; }
    }

    private ZFrameHandlerAsync MessageSinkHandler(IZConnection connection, ZmtpParser parser,
        ReceiveMaterializer? materializer)
    {
        var accumulator = new PeerAccumulator { Materializer = materializer };
        lock (StateLock)
        {
            accumulators[connection] = accumulator;
        }

        return (frame, _) => OnMessageSinkFrameAsync(connection, accumulator, frame, Cts.Token);
    }

    private async ValueTask<bool> OnMessageSinkFrameAsync(
        IZConnection connection,
        PeerAccumulator accumulator,
        ZFrame frame,
        CancellationToken token)
    {
        if (frame.TryGetValue(out ZSegments segments))
        {
            accumulator.Frames.Add(new ZFrame(segments, frame.More));
        }
        else if (frame.TryGetValue(out ZSegment segment) && !segment.IsBorrowed)
        {
            accumulator.Frames.Add(new ZFrame(segment, frame.More));
        }
        else
        {
            frame.TryGetValue(out ZSegment borrowed);
            var poolOwner = Pool.Rent(borrowed.Memory.Length);
            borrowed.Memory.CopyTo(poolOwner.Memory);
            accumulator.Frames.Add(new ZFrame(
                new ZSegment(poolOwner, 0, borrowed.Memory.Length),
                frame.More));
        }

        if (frame.More) return true;

        var message = BuildMessage(accumulator.Frames);
        accumulator.Frames.Clear();
        accumulator.Materializer?.Reset();
        var decision = await inbound.DecideAsync(connection, message, token);
        if (decision.Action != ZInboundAction.Deliver)
            // Drop and Consumed own the message (0019 section 3); the pump
            // continues. The peer's pump stays alive.
            return true;

        var toDeliver = decision.Message ?? message;
        if (messageSink is null)
        {
            // A non-default inbound policy delivering without a bound sink
            // (the aggregated tier has no consumer): drop the message.
            toDeliver.Dispose();
            return true;
        }

        await messageSink.OnMessageAsync(connection, toDeliver, token);
        return true;
    }

    private static ZMessage BuildMessage(List<ZFrame> frames)
    {
        if (frames.Count == 1) return new ZMessage(new ZSingleMessage(frames[0]));

        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    /// <summary>
    /// Per-connection receive materialization (0007 2.1): allocation decisions
    /// via the receive policy plus the connection-level limits (0008 D1),
    /// enforced before any allocation. Frame/message guard counters advance per
    /// frame and reset at each message boundary (a message in excess is
    /// rejected, never leaked into the next message).
    /// </summary>
    private sealed class ReceiveMaterializer(
        MemoryPool<byte> pool,
        IZReceivePolicy policy,
        long maxFrameLength,
        long maxMessageLength,
        int maxFramesPerMessage,
        Action onRejected)
    {
        private const int SegmentBlockSize = 8192;

        private int frameIndex;
        private long accumulatedLength;

        public ZFrameAllocator CreateAllocator()
        {
            return (length, more) =>
            {
                var frameIndex = this.frameIndex;
                if (!ZReceiveGuard.TryAccumulate(accumulatedLength, length, out var accumulated))
                    // An unrepresentable message total is a rejection, never an
                    // arithmetic exception (0008 D3/D6).
                    Reject(ZReceiveRejectionReason.MessageTooLarge, null, null);

                this.frameIndex = frameIndex + 1;
                accumulatedLength = accumulated;

                // Connection-level limits are enforced here, before any allocation,
                // so a custom policy can never bypass them (0008 D1).
                if (ZReceiveGuard.CheckLimits(
                        length,
                        accumulated,
                        frameIndex,
                        maxFrameLength,
                        maxMessageLength,
                        maxFramesPerMessage) is { } rejection)
                    Reject(rejection);

                var allocation = policy.Decide(new ZReceiveContext
                {
                    FrameLength = length,
                    HasMore = more,
                    FrameIndex = frameIndex,
                    AccumulatedLength = accumulated
                });
                return AllocateSegments(allocation, length, more);
            };
        }

        /// <summary>Resets the guard counters at a message boundary.</summary>
        public void Reset()
        {
            frameIndex = 0;
            accumulatedLength = 0;
        }

        private void Reject(ZReceiveRejection rejection)
        {
            onRejected();
            throw new ZReceiveRejectedException(rejection);
        }

        private void Reject(ZReceiveRejectionReason reason, long? limit, long? actual)
        {
            Reject(new ZReceiveRejection { Reason = reason, Limit = limit, Actual = actual });
        }

        private (object Owner, int Length) Allocate(ZReceiveMode mode, int length)
        {
            if (mode == ZReceiveMode.Owned)
            {
                var buffer = GC.AllocateUninitializedArray<byte>(length);
                return (buffer, length);
            }

            var owner = pool.Rent(length);
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

    /// <summary>
    /// Publishes a peer into the routable snapshot; must be called while
    /// holding <see cref="ZAsyncState.StateLock"/>. Copy-on-write: the read
    /// path is a single volatile load (0006 3.6).
    /// </summary>
    private void PublishAdd(IZConnection connection, object? endpoint)
    {
        var updated = new IZConnection[peerSnapshot.Length + 1];
        peerSnapshot.CopyTo(updated, 0);
        updated[^1] = connection;
        peerSnapshot = updated;
        endpoints[connection] = endpoint;
    }

    private void PublishRemove(IZConnection connection)
    {
        var current = peerSnapshot;
        var index = Array.IndexOf(current, connection);
        if (index < 0) return;

        var updated = new IZConnection[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        peerSnapshot = updated;
        endpoints.Remove(connection);
    }
}
