using System.Collections.Concurrent;
using System.Net.Sockets;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Pattern-agnostic transport core (0007 section 2.1): calls the transport,
/// stores connections, drives the handshake externally, aggregates messages
/// for a bound <see cref="IPatternSink"/>, and delivers borrowed frames to the
/// raw OnFrame callback. Pattern semantics live in a composed
/// <see cref="IPatternCore"/>; socket types are thin composition roots.
/// </summary>
public abstract class ZSocketBase : ZAsyncState, IZCallbackSocket
{
    /// <summary>
    /// Copy-on-write routable-peer snapshot: rebuilt only when a peer is
    /// added or removed, read as a single volatile load on the hot path, so
    /// a send never allocates a peer list (0006 3.6).
    /// </summary>
    private volatile IZConnection[] peerSnapshot = [];

    /// <summary>Per-connection endpoint, used only by the rare DisconnectAsync lookup.</summary>
    private readonly Dictionary<IZConnection, object?> endpoints = [];
    private readonly List<(IZTransport Listener, object? Endpoint)> listeners = [];
    private readonly Dictionary<IZConnection, TaskCompletionSource> establishedGates = [];
    private readonly Dictionary<IZConnection, CancellationTokenSource> attemptTokens = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];

    /// <summary>Per-peer frame accumulators for message aggregation (0007 2.3).</summary>
    private readonly Dictionary<IZConnection, PeerAccumulator> accumulators = [];
    private readonly int maxCommandSize;
    private readonly IPatternCore core;
    private ZFrameHandler? onFrame;
    private IPatternSink? messageSink;
    private Func<IZConnection, ZFrameAllocator>? allocatorFactory;
    private Action<IZConnection>? peerConnected;
    private Action<IZConnection, Exception?>? peerEnded;

    internal ZSocketBase(ZSocketOptions options, IPatternCore core)
        : base(options.Pool)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(core);
        this.core = core;
        maxCommandSize = options.MaxCommandSize;
    }

    public event ZFrameHandler? OnFrame
    {
        add
        {
            lock (StateLock)
            {
                // Exactly one consumer of the delivery stream (0007 section 1):
                // a message sink is mutually exclusive with the raw frame
                // surface on the same instance.
                if (messageSink is not null)
                {
                    throw new InvalidOperationException("cannot subscribe to OnFrame after a message sink is bound");
                }

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
        while (paused.TryDequeue(out var parser))
        {
            parser.Resume();
        }
    }

    /// <summary>
    /// Registers a per-peer connection callback for the message-sink surface
    /// (creates the surface's per-connection state, e.g. the queue surface's
    /// PeerState); must be called before any connection is established.
    /// </summary>
    internal void SetPeerConnectedHandler(Action<IZConnection> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
            {
                throw new InvalidOperationException("peer connected handler must be set before connections are established");
            }

            peerConnected = handler;
        }
    }

    /// <summary>
    /// Binds the semantic delivery seam (0007 section 2.3): the transport core
    /// aggregates complete messages and delivers them to the sink, per peer and
    /// serialized. Must be called before any connection is established. A bound
    /// sink is mutually exclusive with the raw <see cref="OnFrame"/> surface.
    /// </summary>
    public void BindMessageSink(IPatternSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
            {
                throw new InvalidOperationException("message sink must be bound before connections are established");
            }

            messageSink = sink;
        }
    }

    /// <summary>
    /// Registers a per-peer frame allocator factory; must be called before any
    /// connection is established.
    /// When set, the parser materializes each frame
    /// directly into the allocated buffer instead of the borrowed scratch.
    /// </summary>
    internal void SetFrameAllocator(Func<IZConnection, ZFrameAllocator> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        lock (StateLock)
        {
            if (peerSnapshot.Length > 0)
            {
                throw new InvalidOperationException("frame allocator must be set before connections are established");
            }

            allocatorFactory = factory;
        }
    }

    public async Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
    {
        ThrowIfClosed();
        var connection = await TTransport.ConnectAsync(endpoint, new ZTransportOptions(), token);
        var established = AddConnection(connection, endpoint, token);
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
            {
                if (endpoints.TryGetValue(connection, out var peerEndpoint) && Equals(peerEndpoint, endpoint))
                {
                    matches.Add(connection);
                }
            }

            foreach (var match in matches)
            {
                // Cancel the in-flight attempt first: disposing the transport
                // stream does not reliably interrupt a pending read, which
                // would leave the connection pump stuck and the peer routable.
                if (attemptTokens.TryGetValue(match, out var attemptCts))
                {
                    attemptCts.Cancel();
                }
            }
        }

        foreach (var match in matches)
        {
            match.Dispose();
        }

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await AwaitBackgroundAsync();
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        ThrowIfClosed();
        try
        {
            await SendToTargetAsync(message, token);
        }
        finally
        {
            message.Dispose();
        }
    }

    private async ValueTask SendToTargetAsync(ZMessage message, CancellationToken token)
    {
        var connection = SelectTarget(message);
        if (connection is null)
        {
            return;
        }

        await WaitUntilEstablishedAsync(connection, token);
        try
        {
            await connection.SendAsync(message, token);
        }
        catch (Exception ex) when (ex is ObjectDisposedException or IOException or SocketException)
        {
            // The peer's connection retired between routing and the write
            // (or mid-write); a send to a dying peer is dropped, never a
            // fault. The retirement is already surfaced through PeerEnded
            // (0006 3.6).
        }
    }

    private IZConnection? SelectTarget(ZMessage message)
        => core.RouteOutbound(message, peerSnapshot.AsSpan());

    public async ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ThrowIfClosed();
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, bytes.Length))));
        await SendAsync(message, token);
    }

    private ValueTask AcceptConnection(IZConnection connection, CancellationToken token)
    {
        AddConnection(connection, null);
        return ValueTask.CompletedTask;
    }

    private TaskCompletionSource AddConnection(IZConnection connection, object? endpoint, CancellationToken token = default)
    {
        ZFrameAllocator? allocator;
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

            allocator = allocatorFactory?.Invoke(connection);
            establishedGates[connection] = established;
            // Register before the pump starts. Sends block on the establishment
            // gate until the handshake completes, and DisconnectAsync must
            // always be able to find the peer to cancel the in-flight attempt.
            PublishAdd(connection, endpoint);
            peerConnected?.Invoke(connection);
        }

        var parser = new ZmtpParser(connection, allocator, Pool, maxCommandSize);
        var handler = messageSink is null ? BorrowedSink(parser) : MessageSinkHandler(connection, parser);
        connection.SetFrameHandler((frame, _) => handler(frame, Cts.Token));

        var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(Cts.Token, token);
        lock (StateLock)
        {
            attemptTokens[connection] = attemptCts;
        }

        var pump = RunConnectionAsync(connection, parser, established, attemptCts);
        TrackBackground(pump);
        return established;
    }

    private async Task RunConnectionAsync(
        IZConnection connection,
        ZmtpParser parser,
        TaskCompletionSource established,
        CancellationTokenSource attemptCts)
    {
        Exception? failure = null;
        var attemptToken = attemptCts.Token;
        try
        {
            await connection.WriteAsync(
                ZmtpFrameEncoder.BuildHandshake(ZmtpCommands.BuildReady(core.SocketTypeName)),
                attemptToken);
            if (await parser.EstablishAsync(attemptToken))
            {
                var peerType = parser.PeerSocketType;
                if (peerType is null || !IsCompatibleSocketType(core.SocketTypeName, peerType))
                {
                    // RFC 23: on socket-type validation failure, return an
                    // ERROR command before disconnecting the peer.
                    await connection.SendCommandAsync(ZmtpCommands.BuildError("Invalid socket type"), attemptToken);
                    throw new ZeroMqProtocolException(
                        $"peer socket type '{peerType}' is not compatible with local socket type '{core.SocketTypeName}'");
                }

                established.TrySetResult();
                await parser.ParseAsync(attemptToken);
            }
            else
            {
                var eof = new IOException("peer closed during ZMTP handshake");
                failure = eof;
                established.TrySetException(eof);
            }
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
            PeerAccumulator? accumulator = null;
            lock (StateLock)
            {
                PublishRemove(connection);
                establishedGates.Remove(connection);
                attemptTokens.Remove(connection);
                if (accumulators.Remove(connection, out accumulator))
                {
                    // The peer ended mid-message: owning frames accumulated for
                    // the incomplete message have no consumer and must not leak.
                    foreach (var frame in accumulator.Frames)
                    {
                        frame.Dispose();
                    }
                }
            }

            try
            {
                connection.OnConnectionEnded();
                RaisePeerEnded(connection, failure);
            }
            finally
            {
                parser.Dispose();
                connection.Dispose();
                attemptCts.Dispose();
            }
        }
    }

    private static bool IsCompatibleSocketType(string localType, string peerType) => localType switch
    {
        "PAIR" => peerType == "PAIR",
        "DEALER" => peerType is "DEALER" or "REP" or "ROUTER",
        _ => true,
    };

    private async ValueTask WaitUntilEstablishedAsync(IZConnection connection, CancellationToken token)
    {
        TaskCompletionSource? gate;
        lock (StateLock)
        {
            establishedGates.TryGetValue(connection, out gate);
        }

        // An established peer's gate completed successfully; skipping the
        // WaitAsync keeps the steady-state send path allocation-free (0006 3.6).
        if (gate is null || gate.Task.IsCompletedSuccessfully)
        {
            return;
        }

        await gate.Task.WaitAsync(token);
    }

    /// <summary>
    /// Default per-peer sink for the borrowed tier: invokes the raw OnFrame
    /// callback; a false return pauses this peer's pump until ResumePaused.
    /// </summary>
    private ZFrameHandlerAsync BorrowedSink(ZmtpParser parser)
        => (frame, _) =>
        {
            var keepGoing = RaiseOnFrame(frame);
            if (!keepGoing)
            {
                paused.Enqueue(parser);
            }

            return ValueTask.FromResult(keepGoing);
        };

    private bool RaiseOnFrame(ZFrame frame)
    {
        ZFrameHandler? handler;
        lock (StateLock)
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
            keepGoing &= ((ZFrameHandler)item)(frame, Cts.Token);
        }

        return keepGoing;
    }

    /// <summary>
    /// Aggregates a peer's frames into complete messages for the message sink
    /// (0007 section 2.3). A borrowed frame is copied into a pooled buffer
    /// before it is retained, mirroring the queue surface's previous
    /// accumulation path.
    /// </summary>
    private sealed class PeerAccumulator
    {
        public List<ZFrame> Frames { get; } = [];
    }

    private ZFrameHandlerAsync MessageSinkHandler(IZConnection connection, ZmtpParser parser)
    {
        var accumulator = new PeerAccumulator();
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

        if (frame.More)
        {
            return true;
        }

        var message = BuildMessage(accumulator.Frames);
        accumulator.Frames.Clear();
        await messageSink!.OnMessageAsync(connection, message, token);
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

    private void RaisePeerEnded(IZConnection connection, Exception? failure)
    {
        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    private async Task StopAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0)
        {
            return;
        }

        await Cts.CancelAsync();
        List<IZTransport> listenerSnapshot;
        lock (StateLock)
        {
            listenerSnapshot = [.. listeners.Select(entry => entry.Listener)];
            listeners.Clear();
        }

        foreach (var listener in listenerSnapshot)
        {
            listener.Dispose();
        }

        Cts.Dispose();
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
        if (index < 0)
        {
            return;
        }

        var updated = new IZConnection[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        peerSnapshot = updated;
        endpoints.Remove(connection);
    }
}
