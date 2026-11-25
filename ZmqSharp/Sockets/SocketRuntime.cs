using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using ZmqSharp.Patterns;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Owns endpoint registrations and peer lifecycles. Sessions provide protocol
/// I/O; policies and coordinators decide routing and inbound disposition.
/// Queue projections share the runtime's records and never own another registry.
/// </summary>
internal sealed class SocketRuntime : IZSocket
{
    internal readonly MemoryPool<byte> Pool;
    internal readonly Lock StateLock = new();
    private readonly List<Task> _backgroundTasks = [];
    private readonly CancellationTokenSource _cts = new();
    private int _closed;
    internal CancellationToken LifetimeToken { get; }
    private bool _lifecycleDisposed;

    internal void TrackBackground(Task task)
    {
        lock (StateLock)
        {
            _backgroundTasks.Add(task);
        }
    }

    private async Task AwaitBackgroundAsync()
    {
        Task[] tasks;
        lock (StateLock)
        {
            tasks = [.. _backgroundTasks];
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException) { }
        catch (ZeroMqProtocolException) { }
        finally
        {
            lock (StateLock)
            {
                if (!_lifecycleDisposed && Volatile.Read(ref _closed) == 1)
                {
                    _lifecycleDisposed = true;
                    _cts.Dispose();
                }
            }
        }
    }

    internal void ThrowIfClosed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _closed) == 1, this);
    }

    /// <summary>
    /// Copy-on-write routable-peer snapshot: rebuilt only when a peer is
    /// added or removed, read as a single volatile load on the hot path, so
    /// a send never allocates a peer list (0006 3.6).
    /// </summary>
    private volatile ZPeer[] _peerSnapshot = [];

    /// <summary>Single owner of every registered peer through final cleanup.</summary>
    private readonly Dictionary<ZPeer, PeerRecord> _peers = [];

    private readonly List<ZEndpointRegistration> _listeners = [];
    private readonly ConcurrentQueue<(MessagePauseGate Gate, ZMessage RetainedMessage)> _paused = [];

    private readonly long _maxCommandSize;
    private readonly ZTransportContext _transportContext;
    private readonly IZDispatchPolicy _dispatch;
    private readonly ZSocketType _type;
    private IZInboundPolicy _inbound;
    private readonly IZSecurityMechanism _mechanism;
    private readonly IZWireProtocol? _configuredProtocol;
    private readonly IZWireProtocol _protocol;
    private readonly ReadOnlyMemory<byte> _localReadyBody;
    private readonly int _handshakeTimeoutMs;
    private readonly int _maxIncompleteHandshakes;
    private int _incompleteHandshakes;
    private ZFrameHandler? _onFrame;
    private readonly IPatternSink? _messageSink;
    private Action<ZPeer>? _peerConnected;
    private Action<ZPeer, Exception?>? _peerEnded;

    // Receive materialization (0007 2.1): allocation policy and the 0008 guard
    // limits, applied per connection by a ReceiveMaterializer.
    private readonly IZReceivePolicy? _receivePolicy;
    private readonly long _maxFrameLength = long.MaxValue;
    private readonly long _maxMessageLength = long.MaxValue;
    private readonly int _maxFramesPerMessage = int.MaxValue;
    private long _receiveRejections;

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
    /// (see the composition capabilities).
    /// </summary>
    internal SocketRuntime(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null, bool supportsQueue = false)
    {
        LifetimeToken = _cts.Token;
        ArgumentNullException.ThrowIfNull(options);
        Pool = options.Pool;
        ArgumentNullException.ThrowIfNull(dispatch);
        ArgumentNullException.ThrowIfNull(type);
        _dispatch = dispatch;
        _type = type;
        _inbound = inbound ?? ZInboundPolicy.PassThrough;
        _maxCommandSize = options.MaxCommandSize;
        _transportContext = new ZTransportContext(options.Framing, Pool);
        _decodedPool = new ZRecycledBufferPool(Pool);
        _mechanism = options.Security.Mechanism;
        _configuredProtocol = options.WireProtocol;
        _protocol = options.WireProtocol ?? ZmqProtocol.Instance;
        _handshakeTimeoutMs = options.HandshakeTimeoutMs;
        _maxIncompleteHandshakes = options.MaxIncompleteHandshakes;
        _messageSink = options.MessageSink;
        if ((!supportsQueue || options.MessageSink is not null || options.ReceiveSurface != ZReceiveSurface.Queue) && options.HasQueueConfiguration)
            throw new InvalidOperationException(
                "queue configuration (ReceiveQueueFactory/ReceivePolicy/limits) requires the queue surface; this socket never composes a queue");
        // The local READY body depends only on the socket type and the
        // configured identity; building it once per socket instead of once
        // per connection keeps the handshake cold path allocation-free (0027
        // D6). The Identity property is attached only by types that advertise
        // one (REQ/DEALER/ROUTER, 0025) - the libzmq add_basic_properties gate.
        _localReadyBody = ZmtpCommands.BuildReady(
            type.Name,
            type.AdvertisesIdentity ? options.Identity : ReadOnlyMemory<byte>.Empty);
        if (!supportsQueue || options.MessageSink is not null || options.ReceiveSurface != ZReceiveSurface.Queue)
            return;

        _receivePolicy = options.ReceivePolicy;
        _maxFrameLength = options.MaxFrameLength;
        _maxMessageLength = options.MaxMessageLength;
        _maxFramesPerMessage = options.MaxFramesPerMessage;
        QueueSurface = new ReceiveQueueSurface(this, options);
    }

    internal event Action<ZPeer, ReadOnlyMemory<byte>?>? PeerEstablished;
    internal event Action<ZPeer, Exception?>? PeerRemoved;
    internal ReceiveQueueSurface? QueueSurface { get; }

    internal void ConfigureInbound(IZInboundPolicy policy)
    {
        if (_peers.Count != 0) throw new InvalidOperationException("configure the coordinator before connections");
        _inbound = policy;
    }

    /// <summary>The routable-peer snapshot (dispatch policies read it for outbound selection).</summary>
    internal ZPeer[] PeerSnapshot => _peerSnapshot;

    private bool NeedsAggregation => QueueSurface is not null || _messageSink is not null || !ReferenceEquals(_inbound, ZInboundPolicy.PassThrough);

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
                if (_messageSink is not null || QueueSurface is not null)
                    throw new InvalidOperationException("cannot subscribe to OnFrame after a message sink is bound");

                if (!ReferenceEquals(_inbound, ZInboundPolicy.PassThrough))
                    throw new InvalidOperationException(
                        "cannot subscribe to OnFrame on a socket with a composed inbound policy; bind a message sink");

                _onFrame += value;
            }
        }
        remove
        {
            lock (StateLock)
            {
                _onFrame -= value;
            }
        }
    }

    public event Action<ZPeer, Exception?>? PeerEnded
    {
        add
        {
            lock (StateLock)
            {
                _peerEnded += value;
            }
        }
        remove
        {
            lock (StateLock)
            {
                _peerEnded -= value;
            }
        }
    }

    /// <summary>
    /// Resumes every peer receive loop paused by a false <see cref="OnFrame"/>
    /// return. Pause granularity is the message (0033): the retained message
    /// stays valid until this resume (the copy-to-retain contract) and is
    /// released just before the loop reads on.
    /// </summary>
    public void ResumePaused()
    {
        while (_paused.TryDequeue(out var paused))
        {
            paused.RetainedMessage.Dispose();
            paused.Gate.Release();
        }
    }

    /// <summary>Registers subscription replay before connections are created.</summary>
    internal void SetPeerConnectedHandler(Action<ZPeer> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (StateLock)
        {
            if (_peerSnapshot.Length > 0)
                throw new InvalidOperationException(
                    "peer connected handler must be set before connections are established");

            _peerConnected += handler;
        }
    }

    /// <summary>Total frames rejected by the receive materialization since construction.</summary>
    internal long ReceiveRejectionsCount => Volatile.Read(ref _receiveRejections);

    private void OnMaterializerRejected()
    {
        Interlocked.Increment(ref _receiveRejections);
        MaterializerRejected?.Invoke();
    }

    /// <summary>
    /// Enforces the message-level receive limits for every connection style
    /// (0034): a framing materializer rejects encoded frames before
    /// allocation, and this catches native or custom connections that hand
    /// over whole messages. A no-op when every limit is unlimited.
    /// </summary>
    private void ValidateReceivedMessage(ZMessage message)
    {
        if (_maxFrameLength == long.MaxValue
            && _maxMessageLength == long.MaxValue
            && _maxFramesPerMessage == int.MaxValue)
            return;

        long accumulated = 0;
        for (var i = 0; i < message.Count; i++)
        {
            var length = ZWireFrameLogic.ByteLength(message[i]);
            if (length > int.MaxValue)
                RejectReceive(ZReceiveRejectionReason.FrameTooLarge, _maxFrameLength, length);

            var frameLength = (int)length;
            if (!ZReceiveGuard.TryAccumulate(accumulated, frameLength, out accumulated))
                RejectReceive(ZReceiveRejectionReason.MessageTooLarge, null, null);

            if (ZReceiveGuard.CheckLimits(frameLength, accumulated, i, _maxFrameLength,
                    _maxMessageLength, _maxFramesPerMessage) is { } rejection)
                RejectReceive(rejection);
        }
    }

    private void RejectReceive(ZReceiveRejection rejection)
    {
        OnMaterializerRejected();
        throw new ZReceiveRejectedException(rejection);
    }

    private void RejectReceive(ZReceiveRejectionReason reason, long? limit, long? actual)
    {
        RejectReceive(new ZReceiveRejection { Reason = reason, Limit = limit, Actual = actual });
    }

    public Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZClientTransport<TTransport, TEndpoint>
        => ConnectCoreAsync<TEndpoint, TTransport>(endpoint, null, token);

    public Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZServerTransport<TTransport, TEndpoint>
        => BindCoreAsync<TEndpoint, TTransport>(endpoint, null, token);

    public async Task ConnectAsync(string endpoint, CancellationToken token = default)
    {
        var parsed = await ZEndpointParser.ParseEndpointAsync(endpoint, token);
        switch (parsed)
        {
            case WebSocketEndpoint webSocket:
                await ConnectCoreAsync<WebSocketEndpoint, WebSocketTransport>(webSocket, endpoint, token);
                break;
            case EndPoint socketEndpoint:
                await ConnectCoreAsync<EndPoint, SocketTransport>(socketEndpoint, endpoint, token);
                break;
            default:
                throw new NotSupportedException($"unsupported endpoint in '{endpoint}'");
        }
    }

    public async Task BindAsync(string endpoint, CancellationToken token = default)
    {
        var parsed = await ZEndpointParser.ParseEndpointAsync(endpoint, token);
        switch (parsed)
        {
            case WebSocketEndpoint:
                // The ZWS transport is client-only (0035): there is no
                // hand-rolled WebSocket server to accept with.
                throw new NotSupportedException(
                    $"the ZWS transport is client-only; ws/wss endpoint '{endpoint}' cannot be bound");
            case EndPoint socketEndpoint:
                await BindCoreAsync<EndPoint, SocketTransport>(socketEndpoint, endpoint, token);
                break;
            default:
                throw new NotSupportedException($"unsupported endpoint in '{endpoint}'");
        }
    }

    private async Task ConnectCoreAsync<TEndpoint, TTransport>(TEndpoint endpoint, string? address,
        CancellationToken token) where TTransport : IZClientTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        token.ThrowIfCancellationRequested();
        var connection = await TTransport.ConnectAsync(endpoint, _transportContext, token);
        var established = AddConnection(connection, endpoint, false, token, typeof(TTransport), address);
        await established.Task.WaitAsync(token);
    }

    private async Task BindCoreAsync<TEndpoint, TTransport>(TEndpoint endpoint, string? address,
        CancellationToken token) where TTransport : IZServerTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        token.ThrowIfCancellationRequested();
        var listener = await TTransport.BindAsync(endpoint, _transportContext, token);
        ZEndpointRegistration? registration = null;
        listener.OnAccept += (connection, acceptToken) => AcceptConnection(connection, acceptToken);
        lock (StateLock)
        {
            if (Volatile.Read(ref _closed) == 0 && !token.IsCancellationRequested)
            {
                registration = new ZEndpointRegistration(listener, endpoint, typeof(TTransport), address, LifetimeToken);
                _listeners.Add(registration);
                TrackBackground(registration.Completion.Task);
            }
        }

        if (registration is null)
        {
            listener.Dispose();
            token.ThrowIfCancellationRequested();
            ThrowIfClosed();
            return;
        }

        _ = RunListenerAsync(listener.StartAsync, registration);
    }

    private async Task RunListenerAsync(Func<CancellationToken, ValueTask> startAsync,
        ZEndpointRegistration registration)
    {
        Exception? failure = null;
        try
        {
            await startAsync(registration.Token);
        }
        catch (OperationCanceledException) when (registration.Token.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (registration.Token.IsCancellationRequested) { }
        catch (SocketException) when (registration.Token.IsCancellationRequested) { }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            try
            {
                await registration.FinishAsync();
            }
            catch (Exception ex)
            {
                failure ??= ex;
            }

            lock (StateLock)
            {
                registration.Complete(failure);
                _listeners.Remove(registration);
            }
        }
    }

    /// <summary>Requests listener shutdown without waiting for the accept loop.</summary>
    public void Unbind<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZServerTransport<TTransport, TEndpoint>
        => RequestEndpointStop(endpoint, typeof(TTransport), null, true);

    /// <summary>Requests peer shutdown without waiting for receive callbacks or cleanup.</summary>
    public void Disconnect<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZClientTransport<TTransport, TEndpoint>
        => RequestEndpointStop(endpoint, typeof(TTransport), null, false);

    /// <summary>Stops matching listeners and waits for cleanup. Existing accepted peers remain connected.</summary>
    public ValueTask UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZServerTransport<TTransport, TEndpoint>
        => StopEndpointAsync(endpoint, typeof(TTransport), null, true, token);

    /// <summary>Stops matching peers and waits for cleanup. Do not await this from a targeted peer's callback.</summary>
    public ValueTask DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZClientTransport<TTransport, TEndpoint>
        => StopEndpointAsync(endpoint, typeof(TTransport), null, false, token);

    public ValueTask UnbindAsync(string endpoint, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        return StopEndpointAsync(null, ZEndpointParser.TransportTypeOf(endpoint), endpoint, true, token);
    }

    public ValueTask DisconnectAsync(string endpoint, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        return StopEndpointAsync(null, ZEndpointParser.TransportTypeOf(endpoint), endpoint, false, token);
    }

    private ZEndpointRegistration[] RequestEndpointStop(object? endpoint, Type transport, string? address, bool listening)
    {
        ZEndpointRegistration[] matches;
        lock (StateLock)
        {
            var source = listening ? _listeners : _peers.Values.Where(record => !record.Accepted).Select(record => record.Registration);
            matches =
            [
                .. source.Where(entry => entry.Transport == transport &&
                                         (address is null ? Equals(entry.Endpoint, endpoint) : entry.Address == address)),
            ];
            if (!listening)
                foreach (var record in _peers.Values)
                    if (matches.Contains(record.Registration))
                        MarkStopping(record);
        }

        foreach (var match in matches) match.RequestStop();
        return matches;
    }

    private async ValueTask StopEndpointAsync(object? endpoint, Type transport, string? address, bool listening,
        CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var matches = RequestEndpointStop(endpoint, transport, address, listening);
        await Task.WhenAll(matches.Select(entry => entry.Completion.Task)).WaitAsync(token);
    }

    internal void RetirePeer(ZPeer peer) => RetirePeer(peer, null);

    private void RetirePeer(ZPeer peer, Exception? failure)
    {
        PeerRecord? record;
        lock (StateLock)
        {
            _peers.TryGetValue(peer, out record);
            if (record is not null)
            {
                if (record.Phase < PeerPhase.Stopping) record.Failure ??= failure;
                MarkStopping(record);
            }
        }

        record?.Registration.RequestStop();
    }

    private void MarkStopping(PeerRecord record)
    {
        if (record.Phase >= PeerPhase.Stopping) return;
        record.Phase = PeerPhase.Stopping;
        PublishRemove(record.Peer);
        QueueSurface?.Remove(record);
        // A recorded failure is the establishment's outcome and must reach the
        // awaiting caller (a peer that closed mid-handshake closes the ERROR
        // write too, and "nobody canceled anything" must not surface as a
        // cancellation). A stop without one is an external shutdown request,
        // whose awaited connect reports cancellation (0029 section 3).
        if (record.Failure is { } failure) record.Established.TrySetException(failure);
        else record.Established.TrySetCanceled();
    }

    /// <summary>
    /// Stops the socket: cancels the background work, disposes listeners, and
    /// awaits the connection pumps. The queue surface composes its own teardown
    /// (reclaiming buffered messages) around this in <see
    /// cref="ZQueueSocketBase"/>.
    /// </summary>
    private TaskCompletionSource? _disposed;

    public ValueTask DisposeAsync()
    {
        TaskCompletionSource completion;
        lock (StateLock)
        {
            if (_disposed is { } existing) return new ValueTask(existing.Task);
            completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposed = completion;
        }

        _ = DisposeCoreAsync(completion);
        return new ValueTask(completion.Task);
    }

    private async Task DisposeCoreAsync(TaskCompletionSource completion)
    {
        Exception? failure = null;
        QueueSurface?.Stop();
        try
        {
            await StopAsync();
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        // Even a failing cancellation callback must not skip waiting for peer cleanup.
        try
        {
            await AwaitBackgroundAsync();
        }
        catch (Exception ex)
        {
            failure ??= ex;
        }

        // Peers and sessions are stopped; release the recycled decoded
        // buffers. Owners still held by delivered messages are released when
        // those messages are disposed (a return to a disposed pool releases
        // the underlying buffer directly). A failing owner dispose must still
        // complete the shutdown task.
        try
        {
            _decodedPool.Dispose();
        }
        catch (Exception ex)
        {
            failure ??= ex;
        }

        QueueSurface?.Complete();
        if (failure is { } error) completion.TrySetException(error);
        else completion.TrySetResult();
    }

    /// <summary>
    /// Disposes listeners and cancels background work; the Closed flag is
    /// already set when this runs. Subclasses that stop in their own order
    /// (the queue surface completes and drains its outbound channel before
    /// stopping the connection pumps) call this from their teardown.
    /// </summary>
    private async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;

        ZEndpointRegistration[] registrations;
        lock (StateLock)
        {
            foreach (var record in _peers.Values) MarkStopping(record);
            registrations = [.. _listeners, .. _peers.Values.Select(record => record.Registration)];
        }

        foreach (var registration in registrations) registration.RequestStop();
        await _cts.CancelAsync();
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
    internal async ValueTask SendAsyncCore(ZMessage message, CancellationToken token = default)
    {
        // The closed check is inside the try so a send on a closed socket
        // still disposes the message (a byte-input overload that constructed
        // it just before the call must not leak its rented buffers); a
        // retaining connection took the original, and its receiving chain
        // releases it instead (0033).
        var transferred = false;
        try
        {
            ThrowIfClosed();
            transferred = await SendToTargetsAsync(message, token);
        }
        finally
        {
            if (!transferred) message.Dispose();
        }
    }

    /// <summary>
    /// Broadcast send (0033 retention contract): copying connections consume
    /// the frames during their write; the FIRST retaining connection takes
    /// the original (and with it the disposal obligation), every further
    /// retaining connection gets its own copy - each channel holds a unit
    /// only it will release. Returns true when the original was handed off
    /// and the caller must NOT dispose it.
    /// </summary>
    private async ValueTask<bool> SendToTargetsAsync(ZMessage message, CancellationToken token)
    {
        var snapshot = _peerSnapshot;
        // The policy may select up to every peer; the target buffer is rented
        // so the steady-state path stays GC-allocation-free (ArrayPool reuses
        // the array, 0006 3.6).
        var targets = ArrayPool<ZPeer>.Shared.Rent(snapshot.Length);
        var transferred = false;
        try
        {
            var count = _dispatch.SelectTargets(message, snapshot, targets.AsSpan(0, snapshot.Length));
            for (var i = 0; i < count; i++)
            {
                var target = targets[i];
                if (transferred && RetainsSent(target))
                {
                    // The original already lives in another retaining
                    // channel; this peer needs its own copy (owned by its
                    // receiving chain, not the runtime) from the socket's
                    // pool so diagnostic pools observe the rentals.
                    await SendToPeerAsync(target, CopyMessage(message), token,
                        suppressRetirement: true, disposeMessage: true);
                }
                else if (await SendToPeerAsync(target, message, token))
                {
                    transferred = true;
                }
            }
        }
        finally
        {
            ArrayPool<ZPeer>.Shared.Return(targets);
        }

        return transferred;
    }

    /// <summary>
    /// A pooled deep copy for fan-out to a further retaining connection:
    /// each frame's body rents its own buffer, so the copy's lifetime is
    /// independent of the original (0033).
    /// </summary>
    private ZMessage CopyMessage(ZMessage message)
    {
        var frames = new ZFrame[message.Count];
        try
        {
            for (var i = 0; i < message.Count; i++)
            {
                var sequence = message[i].ToSequence();
                var length = checked((int)sequence.Length);
                var owner = Pool.Rent(length == 0 ? 1 : length);
                try
                {
                    sequence.CopyTo(owner.Memory.Span);
                    frames[i] = new ZFrame(new ZSegment(owner, 0, length), message[i].More);
                }
                catch
                {
                    owner.Dispose();
                    throw;
                }
            }

            return frames.Length == 1
                ? new ZMessage(new ZSingleMessage(frames[0]))
                : new ZMessage(new ZMultiMessage(frames));
        }
        catch
        {
            foreach (var frame in frames) frame.Dispose();

            throw;
        }
    }

    /// <summary>
    /// Codec sessions on sink/callback sockets (no receive materializer)
    /// still need owned decoded frames: their consumers may retain messages
    /// arbitrarily long, which the session's staged window forbids (0033
    /// review). One pooled segment per frame; disposal returns it.
    /// </summary>
    private ZFrameAllocator PooledDecodedAllocator()
    {
        // Frames handed to a consumer that may retain them arbitrarily need
        // owned buffers; renting from the socket pool per frame would cost a
        // wrapper object per rent (a GC allocation the receive budget would
        // see), so the runtime recycles its decoded owners exactly like the
        // framing's copy pool: handle objects allocated once, returned by
        // the message's dispose.
        return (length, more) => new ZFrame(new ZSegment(RentDecoded(length), 0, length), more);
    }

    /// <summary>
    /// Codec sessions allocate decoded frames through the materializer, whose
    /// per-message counters must reset at the last frame of every message. The
    /// encoded framing's boundary callback is not installed in codec mode, so
    /// the reset travels with the allocator itself.
    /// </summary>
    private ZFrameAllocator ResettingDecodedAllocator(ReceiveMaterializer materializer)
    {
        var allocate = materializer.CreateAllocator();
        return (length, more) =>
        {
            var frame = allocate(length, more);
            if (!more) materializer.Reset();
            return frame;
        };
    }

    private readonly ZRecycledBufferPool _decodedPool;

    private IMemoryOwner<byte> RentDecoded(int length) => _decodedPool.Rent(length);

    private bool RetainsSent(ZPeer peer)
    {
        lock (StateLock)
        {
            return _peers.TryGetValue(peer, out var record) && record.Session is { RetainsSentMessages: true };
        }
    }

    /// <summary>
    /// Establishes the wire protocol within <c>HandshakeTimeoutMs</c> (0006
    /// 3.2); a timed-out establishment faults with a
    /// <see cref="TimeoutException"/>. It covers the greeting and the whole
    /// mechanism command sequence, so a peer that stalls mid-handshake faults
    /// exactly as before (0016 section 8).
    /// </summary>
    private async Task<ZMechanismResult?> EstablishWithTimeoutAsync(
        PeerRecord record, IZWireProtocol protocol)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(record.Registration.Token);
        if (_handshakeTimeoutMs > 0) timeout.CancelAfter(_handshakeTimeoutMs);
        await using var abort = timeout.Token.UnsafeRegister(static state =>
        {
            if (state is IZMessageConnection frames) frames.Abort();
        }, record.Frames);
        try
        {
            return await protocol.EstablishAsync(
                record.Frames, _mechanism, _localReadyBody, _maxCommandSize, timeout.Token);
        }
        catch (OperationCanceledException) when (!record.Registration.Token.IsCancellationRequested)
        {
            throw new TimeoutException("ZMTP handshake timed out");
        }
    }

    /// <summary>
    /// Directed send to a specific connection (0007 section 2.1 primitive,
    /// used by REP reply routing and ROUTER identity addressing). The message
    /// is disposed after the send, exactly once.
    /// </summary>
    internal void SendTracked(ZPeer peer, ZMessage message)
    {
        _ = ObserveSendAsync(peer, message);
    }

    private async Task ObserveSendAsync(ZPeer peer, ZMessage message)
    {
        try
        {
            await SendToAsync(peer, message, LifetimeToken);
        }
        catch (Exception)
        {
            RetirePeer(peer);
        }
    }

    internal ValueTask<bool> SendToAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        => SendToPeerAsync(peer, message, token, disposeMessage: true);

    /// <summary>
    /// Borrows a framed request until the write finishes; the request core
    /// owns disposal unless a retaining connection took the message (the
    /// return value is the retention signal).
    /// </summary>
    internal ValueTask<bool> SendRequestToAsync(ZPeer peer, ZMessage message, CancellationToken token)
    {
        ThrowIfClosed();
        return SendToPeerAsync(peer, message, token, false);
    }

    private async ValueTask<bool> SendToPeerAsync(ZPeer peer, ZMessage message, CancellationToken token,
        bool suppressRetirement = true, bool disposeMessage = false)
    {
        PeerRecord? record = null;
        var leased = false;
        var retained = false;
        try
        {
            token.ThrowIfCancellationRequested();
            ThrowIfClosed();
            lock (StateLock)
            {
                if (!_peers.TryGetValue(peer, out record) || record.Phase >= PeerPhase.Stopping)
                {
                    if (suppressRetirement) return false;
                    throw new IOException("peer retired before send");
                }

                record.ActiveSends++;
                leased = true;
            }

            if (!record.Established.Task.IsCompletedSuccessfully)
                await record.Established.Task.WaitAsync(token);
            if (record.Session is not { } session) throw new IOException("peer has no established session");
            await session.SendAsync(message, token);
            // A completed send on a retaining connection transferred
            // ownership: the receiving chain releases it (0033). The signal
            // is independent of disposeMessage - broadcast callers decide
            // disposal from the return value, directed callers from the skip
            // below.
            retained = session.RetainsSentMessages;
        }
        catch (Exception ex) when (suppressRetirement &&
                                   ex is ObjectDisposedException or IOException or SocketException
                                       or WebSocketException)
        { }
        catch (OperationCanceledException) when (suppressRetirement && record is { Registration.Token.IsCancellationRequested: true }) { }
        finally
        {
            // Owned background sends release their message before teardown can
            // dispose the session - unless the connection retained it (a
            // thrown send never transferred ownership, so failures dispose).
            try
            {
                if (disposeMessage && !retained) message.Dispose();
            }
            finally
            {
                if (leased && record is not null)
                    lock (StateLock)
                    {
                        record.ActiveSends--;
                        if (record.ActiveSends == 0) record.SendsDrained?.TrySetResult();
                    }
            }
        }

        return retained;
    }

    /// <summary>
    /// Bytes overload of the selective send: pools a copy of the payload and
    /// routes it through <see cref="SendAsyncCore(ZMessage, CancellationToken)"/>.
    /// Protected; the public send surface is decided by each socket type (0024).
    /// </summary>
    internal async ValueTask SendAsyncCore(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ThrowIfClosed();
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, bytes.Length))));
        await SendAsyncCore(message, token);
    }

    private ValueTask AcceptConnection(IZMessageConnection connection, CancellationToken token)
    {
        // Accepted peers outlive their listener (StringEndpoints_Unbind...):
        // the listener's accept token must NOT link into the peer's
        // registration - stopping the listener keeps established peers.
        _ = token;
        AddConnection(connection, null, true);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// One connection path for both transport styles (0034): a byte transport
    /// already framed its connection with the socket's dialect, and a native
    /// message transport already owns whole units. The materializer's
    /// allocator is installed after establishment by
    /// <see cref="IZMessageConnection.ConfigureReceive"/>.
    /// </summary>
    private TaskCompletionSource AddConnection(IZMessageConnection connection, object? endpoint,
        bool accepted, CancellationToken token = default, Type? transport = null, string? address = null)
    {
        if (connection.ByteOrigin is null
            && !ReferenceEquals(_transportContext.Framing, ZmtpFraming.Instance))
        {
            connection.Dispose();
            var rejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rejected.TrySetException(new InvalidOperationException(
                "a native message connection already owns its units; a framing cannot be applied"));
            return rejected;
        }

        ReceiveMaterializer? materializer = null;
        if (_receivePolicy is not null)
            materializer = new ReceiveMaterializer(Pool, _receivePolicy, _maxFrameLength, _maxMessageLength,
                _maxFramesPerMessage, OnMaterializerRejected);

        return AddPeer(connection, materializer, endpoint, accepted, token, transport, address);
    }

    /// <summary>
    /// The single record-creation path: the connection exists before the
    /// record, so the registration's resource and abort are the connection
    /// itself.
    /// </summary>
    private TaskCompletionSource AddPeer(IZMessageConnection frames,
        ReceiveMaterializer? materializer, object? endpoint, bool accepted, CancellationToken token,
        Type? transport, string? address)
    {
        PeerRecord? record = null;
        lock (StateLock)
        {
            if (Volatile.Read(ref _closed) == 0 &&
                (!accepted || _maxIncompleteHandshakes <= 0 || _incompleteHandshakes < _maxIncompleteHandshakes))
            {
                var registration = new ZEndpointRegistration(frames, endpoint, transport, address, LifetimeToken, token,
                    frames.Abort);
                record = new PeerRecord(frames, registration, accepted) { Materializer = materializer };
                _peers.Add(record.Peer, record);
                if (accepted) _incompleteHandshakes++;
                TrackBackground(registration.Completion.Task);
                PublishAdd(record.Peer);
            }
        }

        if (record is null)
        {
            frames.Dispose();
            var rejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rejected.TrySetCanceled();
            return rejected;
        }

        // Establishment bound: a command frame header declaring more than the
        // socket's limit rejects before any body is staged (0008 Slice B).
        // The traffic composition replaces it after establishment. The
        // establishment exchange is command/identity-only; two frames are
        // allowed so the no-mechanism identity shape check can report its
        // specific error, while a longer MORE chain is rejected while it is
        // still being read.
        frames.ConfigureReceive(new ZReceiveComposition(
            allocator: null, _maxCommandSize, onMessageBoundary: null,
            maxMessageLength: _maxCommandSize, maxFramesPerMessage: 2,
            onRejected: OnMaterializerRejected));

        _ = RunConnectionAsync(record);
        return record.Established;
    }

    private async Task RunConnectionAsync(PeerRecord record)
    {
        Exception? failure = null;
        Exception? cleanupFailure = null;
        var token = record.Registration.Token;
        try
        {
            lock (StateLock)
            {
                if (record.Phase >= PeerPhase.Stopping) throw new OperationCanceledException(token);
                record.Phase = PeerPhase.Handshaking;
            }

            // The wire protocol drives establishment and the session over the
            // connection (0034). A connection that negotiates its own protocol
            // in the transport (ZWS, 0035) supplies it here; an explicit
            // socket-level WireProtocol still wins.
            var protocol = _configuredProtocol
                           ?? (record.Frames is IZProtocolCarrier carrier ? carrier.Protocol : _protocol);
            var result = await EstablishWithTimeoutAsync(record, protocol);
            if (result is not { } established) throw new IOException("peer closed during handshake");

            // Post-establishment receive composition (0034): the codec is
            // known only now. A byte-backed framing swaps its reader policy in
            // place - one scratch per connection. NULL keeps the zero-copy
            // traffic path (allocator); a codec session unbounds the framing
            // to the encoded maximum and materializes decoded frames through
            // the session's allocator instead. Native message connections
            // already own their frames and ignore the composition.
            if (established.Codec is { } codec)
            {
                record.Frames.ConfigureReceive(new ZReceiveComposition(
                    allocator: null,
                    codec.GetMaximumEncodedLength(Math.Max(_maxFrameLength, _maxCommandSize)),
                    onMessageBoundary: null,
                    onRejected: OnMaterializerRejected));
            }
            else
            {
                // No codec: the allocator owns the semantic limits and rejects
                // before renting; the boundary callback resets its counters at
                // every complete wire message. A byte framing keeps the wide
                // bound so the allocator remains the rejection source (the
                // MaterializerRejected signal); a native connection that
                // re-aggregates frames enforces the real limits while reading.
                var native = record.Frames.ByteOrigin is null;
                record.Frames.ConfigureReceive(new ZReceiveComposition(
                    record.Materializer?.CreateAllocator(),
                    native ? _maxFrameLength : long.MaxValue,
                    record.Materializer is { } materializer ? materializer.Reset : null,
                    _maxMessageLength,
                    _maxFramesPerMessage,
                    OnMaterializerRejected));
            }

            record.Session = protocol.CreateSession(
                record.Frames, established.Codec, fault => RetirePeer(record.Peer, fault),
                established.Codec is null
                    ? null
                    : record.Materializer is { } resetMaterializer
                        ? ResettingDecodedAllocator(resetMaterializer)
                        : PooledDecodedAllocator(),
                _maxCommandSize, _maxFrameLength, Pool);
            ReadOnlyMemory<byte>? peerIdentity;
            if (established.PeerIdentity is { } advertisedIdentity)
            {
                // The no-mechanism profile exchanged a routing identity
                // message instead of READY metadata; without a Socket-Type
                // property the RFC 45 compatibility check has no wire source
                // and is skipped (0035).
                peerIdentity = advertisedIdentity.Length == 0 ? null : advertisedIdentity;
            }
            else
            {
                var peerType = ZmtpCommandCodec.ParseReadySocketType(established.PeerReadyBody.Span);
                if (!_type.AcceptsPeer(peerType))
                {
                    await record.Session.SendCommandAsync(ZmtpCommands.BuildError("Invalid socket type"), token);
                    throw new ZeroMqProtocolException($"peer socket type '{peerType}' is not accepted by local socket type '{_type.Name}'");
                }

                peerIdentity = ZmtpCommandCodec.ParseReadyIdentity(established.PeerReadyBody.Span);
            }

            PeerEstablished?.Invoke(record.Peer, peerIdentity);
            var aggregate = NeedsAggregation;
            lock (StateLock)
            {
                if (record.Phase >= PeerPhase.Stopping) throw new OperationCanceledException(token);
                record.Phase = PeerPhase.Established;
                if (record.HandshakeCounted) _incompleteHandshakes--;
                record.HandshakeCounted = false;
                QueueSurface?.Add(record);
                record.Established.TrySetResult();
            }

            // The receive loop (0033): the session yields complete messages -
            // the framing aggregated the frames, the session applied any
            // codec - and the runtime routes each whole message. The
            // frame-level pump no longer exists.
            _peerConnected?.Invoke(record.Peer);
            while (await record.Session.ReadMessageAsync(token) is { } message)
            {
                bool keepGoing;
                try
                {
                    ValidateReceivedMessage(message);
                    keepGoing = aggregate
                        ? await DeliverMessageAsync(record, message, token)
                        : DissectMessage(record, message, token);
                }
                catch
                {
                    // A throwing consumer must not leak the delivered message;
                    // the peer faults with the original exception either way.
                    message.Dispose();
                    throw;
                }

                if (!aggregate)
                {
                    // The dissected frames were handed to the raw callbacks
                    // synchronously; the message is consumed (the old borrowed
                    // tier's valid-until-the-pump-continues semantics).
                    if (keepGoing)
                    {
                        message.Dispose();
                        continue;
                    }
                }
                else if (keepGoing) continue;

                // Pause: the retained message stays valid until ResumePaused
                // releases it (0033); a peer that tears down while paused
                // releases it here - the stale queue entry drained later is
                // a harmless second dispose.
                var gate = new MessagePauseGate();
                _paused.Enqueue((gate, message));
                try
                {
                    await gate.WaitAsync(token);
                }
                catch (OperationCanceledException)
                {
                    message.Dispose();
                    throw;
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested || record.Phase >= PeerPhase.Stopping)
        {
            record.Established.TrySetCanceled(token);
        }
        catch (Exception ex)
        {
            if (!token.IsCancellationRequested || ex is ZeroMqProtocolException) failure = ex;
            record.Established.TrySetException(ex);
            if (record.Accepted) _ = record.Established.Task.Exception;
        }
        finally
        {
            Task sends;
            lock (StateLock)
            {
                failure ??= record.Failure;
                MarkStopping(record);
                if (record.HandshakeCounted) _incompleteHandshakes--;
                sends = record.ActiveSends == 0
                    ? Task.CompletedTask
                    : (record.SendsDrained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            }

            record.Registration.RequestStop();
            await sends;

            void Clean(Action action)
            {
                try
                {
                    action();
                }
                catch (Exception ex)
                {
                    cleanupFailure ??= ex;
                }
            }

            Clean(() => PeerRemoved?.Invoke(record.Peer, failure));
            Clean(() => QueueSurface?.Reclaim(record, failure));
            try
            {
                await record.Registration.FinishAsync();
            }
            catch (Exception ex)
            {
                cleanupFailure ??= ex;
            }

            Clean(() => record.Session?.Dispose());
            Clean(record.Frames.Dispose);
            try
            {
                RaisePeerEnded(record.Peer, failure);
            }
            catch (Exception ex)
            {
                cleanupFailure ??= ex;
            }
            finally
            {
                lock (StateLock)
                {
                    record.Phase = PeerPhase.Closed;
                    record.Registration.Complete(cleanupFailure);
                    _peers.Remove(record.Peer);
                }
            }
        }
    }

    /// <summary>
    /// The raw OnFrame tier (0033): a complete message is dissected into its
    /// (already materialized) frames and each is handed to the raw callback
    /// synchronously; a false return pauses the loop until ResumePaused,
    /// retaining the message. Frames of a completed message are stable -
    /// steadier than the old mid-stream borrowed views.
    /// </summary>
    private bool DissectMessage(PeerRecord record, ZMessage message, CancellationToken token)
    {
        _ = record;
        var keepGoing = true;
        ZFrameHandler? handler;
        lock (StateLock)
        {
            handler = _onFrame;
        }

        if (handler is not null)
            foreach (var item in Delegate.EnumerateInvocationList(handler))
            {
                for (var i = 0; i < message.Count; i++)
                {
                    keepGoing &= item(message[i], token);
                    if (!keepGoing) return false;
                }
            }

        return keepGoing;
    }

    /// <summary>
    /// Routes one complete message through the inbound policy to the queue
    /// surface or the bound message sink (0007 section 2.3, 0019 section 3):
    /// the aggregation dissolved into the framing, so the message arrives
    /// whole.
    /// </summary>
    private async ValueTask<bool> DeliverMessageAsync(PeerRecord record, ZMessage message,
        CancellationToken token)
    {
        var connection = record.Peer;
        var decision = await _inbound.DecideAsync(connection, message, token);
        if (decision.Action != ZInboundAction.Deliver)
            // Drop and Consumed own the message (0019 section 3); the loop
            // reads on.
            return true;

        var toDeliver = decision.Message ?? message;
        if (QueueSurface is not null)
        {
            await QueueSurface.DeliverAsync(record, toDeliver, token);
            return true;
        }

        if (_messageSink is null)
        {
            // A non-default inbound policy delivering without a bound sink
            // (the aggregated tier has no consumer): drop the message.
            toDeliver.Dispose();
            return true;
        }

        await _messageSink.OnMessageAsync(connection, toDeliver, token);
        return true;
    }

    /// <summary>A resumable pause of one peer's receive loop (message-granular, 0033).</summary>
    internal sealed class MessagePauseGate
    {
        private readonly TaskCompletionSource _resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => _resumed.TrySetResult();

        public Task WaitAsync(CancellationToken token) => _resumed.Task.WaitAsync(token);
    }

    /// <summary>Publishes termination after internal cleanup; never participates in cleanup.</summary>
    private void RaisePeerEnded(ZPeer connection, Exception? failure)
    {
        Action<ZPeer, Exception?>? handler;
        lock (StateLock)
        {
            handler = _peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    /// <summary>
    /// Publishes a peer into the routable snapshot; must be called while
    /// holding <see cref="StateLock"/>. Copy-on-write: the read
    /// path is a single volatile load (0006 3.6).
    /// </summary>
    private void PublishAdd(ZPeer connection)
    {
        var updated = new ZPeer[_peerSnapshot.Length + 1];
        _peerSnapshot.CopyTo(updated, 0);
        updated[^1] = connection;
        _peerSnapshot = updated;
    }

    private void PublishRemove(ZPeer connection)
    {
        var current = _peerSnapshot;
        var index = Array.IndexOf(current, connection);
        if (index < 0) return;

        var updated = new ZPeer[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        _peerSnapshot = updated;
    }
}
