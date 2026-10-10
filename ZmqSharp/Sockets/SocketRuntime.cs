using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
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
    private readonly List<Task> backgroundTasks = [];
    private readonly CancellationTokenSource cts = new();
    private int closed;
    internal CancellationToken LifetimeToken { get; }
    private bool lifecycleDisposed;

    internal void TrackBackground(Task task)
    {
        lock (StateLock)
        {
            backgroundTasks.Add(task);
        }
    }

    private async Task AwaitBackgroundAsync()
    {
        Task[] tasks;
        lock (StateLock)
        {
            tasks = [.. backgroundTasks];
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
                if (!lifecycleDisposed && Volatile.Read(ref closed) == 1)
                {
                    lifecycleDisposed = true;
                    cts.Dispose();
                }
            }
        }
    }

    internal void ThrowIfClosed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) == 1, this);
    }

    /// <summary>
    /// Copy-on-write routable-peer snapshot: rebuilt only when a peer is
    /// added or removed, read as a single volatile load on the hot path, so
    /// a send never allocates a peer list (0006 3.6).
    /// </summary>
    private volatile ZPeer[] peerSnapshot = [];

    /// <summary>Single owner of every registered peer through final cleanup.</summary>
    private readonly Dictionary<ZPeer, PeerRecord> peers = [];

    private readonly List<ZEndpointRegistration> listeners = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];

    private readonly long maxCommandSize;
    private readonly IZDispatchPolicy dispatch;
    private readonly ZSocketType type;
    private IZInboundPolicy inbound;
    private readonly IZSecurityMechanism mechanism;
    private readonly ReadOnlyMemory<byte> localReadyBody;
    private readonly int handshakeTimeoutMs;
    private readonly int maxIncompleteHandshakes;
    private int incompleteHandshakes;
    private ZFrameHandler? onFrame;
    private readonly IPatternSink? messageSink;
    private Action<ZPeer>? peerConnected;
    private Action<ZPeer, Exception?>? peerEnded;

    // Receive materialization (0007 2.1): allocation policy and the 0008 guard
    // limits, applied per connection by a ReceiveMaterializer.
    private readonly IZReceivePolicy? receivePolicy;
    private readonly long maxFrameLength = long.MaxValue;
    private readonly long maxMessageLength = long.MaxValue;
    private readonly int maxFramesPerMessage = int.MaxValue;
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
    /// (see the composition capabilities).
    /// </summary>
    internal SocketRuntime(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null, bool supportsQueue = false)
    {
        LifetimeToken = cts.Token;
        ArgumentNullException.ThrowIfNull(options);
        Pool = options.Pool;
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
        if ((!supportsQueue || options.MessageSink is not null || options.ReceiveSurface != ZReceiveSurface.Queue) && options.HasQueueConfiguration)
            throw new InvalidOperationException(
                "queue configuration (ReceiveQueueFactory/ReceivePolicy/limits) requires the queue surface; this socket never composes a queue");
        // The local READY body depends only on the socket type and the
        // configured identity; building it once per socket instead of once
        // per connection keeps the handshake cold path allocation-free (0027
        // D6). The Identity property is attached only by types that advertise
        // one (REQ/DEALER/ROUTER, 0025) - the libzmq add_basic_properties gate.
        localReadyBody = ZmtpCommands.BuildReady(
            type.Name,
            type.AdvertisesIdentity ? options.Identity : ReadOnlyMemory<byte>.Empty);
        if (!supportsQueue || options.MessageSink is not null || options.ReceiveSurface != ZReceiveSurface.Queue)
            return;

        receivePolicy = options.ReceivePolicy;
        maxFrameLength = options.MaxFrameLength;
        maxMessageLength = options.MaxMessageLength;
        maxFramesPerMessage = options.MaxFramesPerMessage;
        QueueSurface = new ReceiveQueueSurface(this, options);
    }

    internal event Action<ZPeer, ReadOnlyMemory<byte>?>? PeerEstablished;
    internal event Action<ZPeer, Exception?>? PeerRemoved;
    internal ReceiveQueueSurface? QueueSurface { get; }

    internal void ConfigureInbound(IZInboundPolicy policy)
    {
        if (peers.Count != 0) throw new InvalidOperationException("configure the coordinator before connections");
        inbound = policy;
    }

    /// <summary>The routable-peer snapshot (dispatch policies read it for outbound selection).</summary>
    internal ZPeer[] PeerSnapshot => peerSnapshot;

    private bool NeedsAggregation => QueueSurface is not null || messageSink is not null || !ReferenceEquals(inbound, ZInboundPolicy.PassThrough);

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
                if (messageSink is not null || QueueSurface is not null)
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

    public event Action<ZPeer, Exception?>? PeerEnded
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

    /// <summary>Registers subscription replay before connections are created.</summary>
    internal void SetPeerConnectedHandler(Action<ZPeer> handler)
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

    /// <summary>Total frames rejected by the receive materialization since construction.</summary>
    internal long ReceiveRejectionsCount => Volatile.Read(ref receiveRejections);

    private void OnMaterializerRejected()
    {
        Interlocked.Increment(ref receiveRejections);
        MaterializerRejected?.Invoke();
    }

    public Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => ConnectCoreAsync<TEndpoint, TTransport>(endpoint, null, token);

    public Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => BindCoreAsync<TEndpoint, TTransport>(endpoint, null, token);

    public async Task ConnectAsync(string endpoint, CancellationToken token = default)
    {
        var parsed = await ZEndpointParser.ParseEndpointAsync(endpoint, token);
        await ConnectCoreAsync<EndPoint, SocketTransport>(parsed, endpoint, token);
    }

    public async Task BindAsync(string endpoint, CancellationToken token = default)
    {
        var parsed = await ZEndpointParser.ParseEndpointAsync(endpoint, token);
        await BindCoreAsync<EndPoint, SocketTransport>(parsed, endpoint, token);
    }

    private async Task ConnectCoreAsync<TEndpoint, TTransport>(TEndpoint endpoint, string? address,
        CancellationToken token) where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        token.ThrowIfCancellationRequested();
        var connection = await TTransport.ConnectAsync(endpoint, token);
        var established = AddConnection(connection, endpoint, false, token, typeof(TTransport), address);
        await established.Task.WaitAsync(token);
    }

    private async Task BindCoreAsync<TEndpoint, TTransport>(TEndpoint endpoint, string? address,
        CancellationToken token) where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        token.ThrowIfCancellationRequested();
        var listener = await TTransport.BindAsync(endpoint, token);
        ZEndpointRegistration? registration = null;
        listener.OnAccept += AcceptConnection;
        lock (StateLock)
        {
            if (Volatile.Read(ref closed) == 0 && !token.IsCancellationRequested)
            {
                registration = new ZEndpointRegistration(listener, endpoint, typeof(TTransport), address, LifetimeToken);
                listeners.Add(registration);
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

        _ = RunListenerAsync(listener, registration);
    }

    private async Task RunListenerAsync(IZTransport listener, ZEndpointRegistration registration)
    {
        Exception? failure = null;
        try
        {
            await listener.StartAsync(registration.Token);
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
                listeners.Remove(registration);
            }
        }
    }

    /// <summary>Requests listener shutdown without waiting for the accept loop.</summary>
    public void Unbind<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => RequestEndpointStop(endpoint, typeof(TTransport), null, true);

    /// <summary>Requests peer shutdown without waiting for receive callbacks or cleanup.</summary>
    public void Disconnect<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => RequestEndpointStop(endpoint, typeof(TTransport), null, false);

    /// <summary>Stops matching listeners and waits for cleanup. Existing accepted peers remain connected.</summary>
    public ValueTask UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => StopEndpointAsync(endpoint, typeof(TTransport), null, true, token);

    /// <summary>Stops matching peers and waits for cleanup. Do not await this from a targeted peer's callback.</summary>
    public ValueTask DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => StopEndpointAsync(endpoint, typeof(TTransport), null, false, token);

    public ValueTask UnbindAsync(string endpoint, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        return StopEndpointAsync(null, typeof(SocketTransport), endpoint, true, token);
    }

    public ValueTask DisconnectAsync(string endpoint, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        return StopEndpointAsync(null, typeof(SocketTransport), endpoint, false, token);
    }

    private ZEndpointRegistration[] RequestEndpointStop(object? endpoint, Type transport, string? address, bool listening)
    {
        ZEndpointRegistration[] matches;
        lock (StateLock)
        {
            var source = listening ? listeners : peers.Values.Where(record => !record.Accepted).Select(record => record.Registration);
            matches =
            [
                .. source.Where(entry => entry.Transport == transport &&
                                         (address is null ? Equals(entry.Endpoint, endpoint) : entry.Address == address))
            ];
            if (!listening)
                foreach (var record in peers.Values)
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
            peers.TryGetValue(peer, out record);
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
    private TaskCompletionSource? disposed;

    public ValueTask DisposeAsync()
    {
        TaskCompletionSource completion;
        lock (StateLock)
        {
            if (disposed is { } existing) return new ValueTask(existing.Task);
            completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            disposed = completion;
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
        if (Interlocked.Exchange(ref closed, 1) != 0) return;

        ZEndpointRegistration[] registrations;
        lock (StateLock)
        {
            foreach (var record in peers.Values) MarkStopping(record);
            registrations = [.. listeners, .. peers.Values.Select(record => record.Registration)];
        }

        foreach (var registration in registrations) registration.RequestStop();
        await cts.CancelAsync();
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
        // it just before the call must not leak its rented buffers).
        try
        {
            ThrowIfClosed();
            await SendToTargetsAsync(message, token);
        }
        finally
        {
            message.Dispose();
        }
    }

    private async ValueTask SendToTargetsAsync(ZMessage message, CancellationToken token)
    {
        var snapshot = peerSnapshot;
        // The policy may select up to every peer; the target buffer is rented
        // so the steady-state path stays GC-allocation-free (ArrayPool reuses
        // the array, 0006 3.6).
        var targets = ArrayPool<ZPeer>.Shared.Rent(snapshot.Length);
        try
        {
            var count = dispatch.SelectTargets(message, snapshot, targets.AsSpan(0, snapshot.Length));
            for (var i = 0; i < count; i++) await SendToPeerAsync(targets[i], message, token);
        }
        finally
        {
            ArrayPool<ZPeer>.Shared.Return(targets);
        }
    }

    /// <summary>
    /// Establishes the ZMTP handshake within <c>HandshakeTimeoutMs</c> (0006
    /// 3.2); a timed-out handshake faults the establishment with a
    /// <see cref="TimeoutException"/>. The handshake covers the greeting and
    /// the whole mechanism command sequence, so a peer that stalls mid-handshake
    /// faults exactly as before (0016 section 8).
    /// </summary>
    private async Task<ZMechanismResult?> EstablishWithTimeoutAsync(ZmtpHandshake handshake,
        PeerRecord record)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(record.Registration.Token);
        if (handshakeTimeoutMs > 0) timeout.CancelAfter(handshakeTimeoutMs);
        await using var abort = timeout.Token.UnsafeRegister(static state =>
        {
            if (state is IZConnection connection) connection.Abort();
        }, record.Connection);
        try
        {
            return await handshake.EstablishAsync(timeout.Token);
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

    internal ValueTask SendToAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        => SendToPeerAsync(peer, message, token, disposeMessage: true);

    /// <summary>Borrows a framed request until the write finishes; the request core owns disposal.</summary>
    internal ValueTask SendRequestToAsync(ZPeer peer, ZMessage message, CancellationToken token)
    {
        ThrowIfClosed();
        return SendToPeerAsync(peer, message, token, false);
    }

    private async ValueTask SendToPeerAsync(ZPeer peer, ZMessage message, CancellationToken token,
        bool suppressRetirement = true, bool disposeMessage = false)
    {
        PeerRecord? record = null;
        var leased = false;
        try
        {
            token.ThrowIfCancellationRequested();
            ThrowIfClosed();
            lock (StateLock)
            {
                if (!peers.TryGetValue(peer, out record) || record.Phase >= PeerPhase.Stopping)
                {
                    if (suppressRetirement) return;
                    throw new IOException("peer retired before send");
                }

                record.ActiveSends++;
                leased = true;
            }

            if (!record.Established.Task.IsCompletedSuccessfully)
                await record.Established.Task.WaitAsync(token);
            if (record.Session is not { } session) throw new IOException("peer has no established session");
            await session.SendAsync(message, token);
        }
        catch (Exception ex) when (suppressRetirement && ex is ObjectDisposedException or IOException or SocketException) { }
        catch (OperationCanceledException) when (suppressRetirement && record is { Registration.Token.IsCancellationRequested: true }) { }
        finally
        {
            // Owned background sends release their message before teardown can dispose the session.
            try
            {
                if (disposeMessage) message.Dispose();
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

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, null, true);
        return ValueTask.CompletedTask;
    }

    private TaskCompletionSource AddConnection(IZConnection connection, object? endpoint,
        bool accepted = false, CancellationToken token = default, Type? transport = null, string? address = null)
    {
        PeerRecord? record = null;
        lock (StateLock)
        {
            if (Volatile.Read(ref closed) == 0 &&
                (!accepted || maxIncompleteHandshakes <= 0 || incompleteHandshakes < maxIncompleteHandshakes))
            {
                var registration = new ZEndpointRegistration(connection, endpoint, transport, address, LifetimeToken, token,
                    connection.Abort);
                record = new PeerRecord(connection, registration, accepted);
                if (receivePolicy is not null)
                    record.Materializer = new ReceiveMaterializer(Pool, receivePolicy, maxFrameLength, maxMessageLength,
                        maxFramesPerMessage, OnMaterializerRejected);
                peers.Add(record.Peer, record);
                if (accepted) incompleteHandshakes++;
                TrackBackground(registration.Completion.Task);
                PublishAdd(record.Peer);
            }
        }

        if (record is null)
        {
            connection.Dispose();
            var rejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rejected.TrySetCanceled();
            return rejected;
        }

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

            using var handshake = new ZmtpHandshake(record.Connection, mechanism, localReadyBody, maxCommandSize, Pool);
            var result = await EstablishWithTimeoutAsync(handshake, record);
            if (result is not { } established) throw new IOException("peer closed during ZMTP handshake");
            record.Session = new ZmtpSession(record.Connection, established.Codec, fault => RetirePeer(record.Peer, fault));
            var peerType = ZmtpCommandCodec.ParseReadySocketType(established.PeerReadyBody.Span);
            if (!type.AcceptsPeer(peerType))
            {
                await record.Session.SendCommandAsync(ZmtpCommands.BuildError("Invalid socket type"), token);
                throw new ZeroMqProtocolException($"peer socket type '{peerType}' is not accepted by local socket type '{type.Name}'");
            }

            PeerEstablished?.Invoke(record.Peer, ZmtpCommandCodec.ParseReadyIdentity(established.PeerReadyBody.Span));
            ZFrameHandlerAsync handler = static (_, _) => ValueTask.FromResult(true);
            var parser = record.Session.CreateParser((frame, ct) => handler(frame, ct),
                record.Materializer?.CreateAllocator(), Pool, maxCommandSize, maxFrameLength);
            handler = NeedsAggregation ? MessageSinkHandler(record) : BorrowedSink(parser);
            lock (StateLock)
            {
                if (record.Phase >= PeerPhase.Stopping) throw new OperationCanceledException(token);
                record.Phase = PeerPhase.Established;
                if (record.HandshakeCounted) incompleteHandshakes--;
                record.HandshakeCounted = false;
                QueueSurface?.Add(record);
                record.Established.TrySetResult();
            }

            peerConnected?.Invoke(record.Peer);
            await parser.ParseAsync(token);
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
                if (record.HandshakeCounted) incompleteHandshakes--;
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
            Clean(() =>
            {
                foreach (var frame in record.Accumulator.Frames) frame.Dispose();
                record.Accumulator.Frames.Clear();
            });
            try
            {
                await record.Registration.FinishAsync();
            }
            catch (Exception ex)
            {
                cleanupFailure ??= ex;
            }

            Clean(() => record.Session?.Dispose());
            Clean(record.Connection.Dispose);
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
                    peers.Remove(record.Peer);
                }
            }
        }
    }

    /// <summary>
    /// Default per-peer sink for the borrowed tier: invokes the raw OnFrame
    /// callback; a false return pauses this peer's pump until ResumePaused.
    /// </summary>
    private ZFrameHandlerAsync BorrowedSink(ZmtpParser parser)
    {
        return (frame, token) =>
        {
            var keepGoing = RaiseOnFrame(frame, token);
            if (!keepGoing) paused.Enqueue(parser);

            return ValueTask.FromResult(keepGoing);
        };
    }

    private bool RaiseOnFrame(ZFrame frame, CancellationToken token)
    {
        ZFrameHandler? handler;
        lock (StateLock)
        {
            handler = onFrame;
        }

        if (handler is null) return true;

        var keepGoing = true;
        foreach (var item in Delegate.EnumerateInvocationList(handler)) keepGoing &= item(frame, token);

        return keepGoing;
    }

    /// <summary>
    /// Aggregates a peer's frames into complete messages for the message sink
    /// (0007 section 2.3). A borrowed frame is copied into a pooled buffer
    /// before it is retained, mirroring the queue surface's previous
    /// accumulation path. The receive materializer's guard counters reset at
    /// each message boundary.
    /// </summary>
    private ZFrameHandlerAsync MessageSinkHandler(PeerRecord record)
        => (frame, token) => OnMessageSinkFrameAsync(record, frame, token);

    private async ValueTask<bool> OnMessageSinkFrameAsync(
        PeerRecord record,
        ZFrame frame,
        CancellationToken token)
    {
        var accumulator = record.Accumulator;
        var connection = record.Peer;
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
        record.Materializer?.Reset();
        var decision = await inbound.DecideAsync(connection, message, token);
        if (decision.Action != ZInboundAction.Deliver)
            // Drop and Consumed own the message (0019 section 3); the pump
            // continues. The peer's pump stays alive.
            return true;

        var toDeliver = decision.Message ?? message;
        if (QueueSurface is not null)
        {
            await QueueSurface.DeliverAsync(record, toDeliver, token);
            return true;
        }

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
        => frames.Count == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage([.. frames]));

    /// <summary>Publishes termination after internal cleanup; never participates in cleanup.</summary>
    private void RaisePeerEnded(ZPeer connection, Exception? failure)
    {
        Action<ZPeer, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
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
        var updated = new ZPeer[peerSnapshot.Length + 1];
        peerSnapshot.CopyTo(updated, 0);
        updated[^1] = connection;
        peerSnapshot = updated;
    }

    private void PublishRemove(ZPeer connection)
    {
        var current = peerSnapshot;
        var index = Array.IndexOf(current, connection);
        if (index < 0) return;

        var updated = new ZPeer[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        peerSnapshot = updated;
    }
}
