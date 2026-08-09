using System.Collections.Concurrent;
using System.Net.Sockets;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared socket mechanics: calls the transport, stores connections, drives
/// the handshake externally, and delivers borrowed frames to the OnFrame
/// callback. Socket types are subtypes that differ only in
/// <see cref="RouteOutbound"/>.
/// </summary>
public abstract class ZSocketBase : ZAsyncState, IZCallbackSocket
{
    private readonly record struct Peer(IZConnection Connection, object? Endpoint);

    private readonly List<Peer> peers = [];
    private readonly List<(IZTransport Listener, object? Endpoint)> listeners = [];
    private readonly Dictionary<IZConnection, TaskCompletionSource> establishedGates = [];
    private readonly Dictionary<IZConnection, CancellationTokenSource> attemptTokens = [];
    private readonly ConcurrentQueue<ZmtpParser> paused = [];
    private ZFrameHandler? onFrame;
    private Func<IZConnection, ZFrameHandlerAsync>? frameSinkFactory;
    private Func<IZConnection, ZFrameAllocator>? allocatorFactory;
    private Action<IZConnection, Exception?>? peerEnded;

    protected ZSocketBase(ZSocketOptions options)
        : base(options.Pool)
    {
        ArgumentNullException.ThrowIfNull(options);
    }

    /// <summary>Selects the outbound connection(s) for a message; empty = drop.</summary>
    protected abstract IReadOnlyList<IZConnection> RouteOutbound(
        ZMessage message,
        IReadOnlyList<IZConnection> peers);

    /// <summary>ZMTP Socket-Type metadata advertised in the READY handshake.</summary>
    protected abstract string SocketTypeName { get; }

    public event ZFrameHandler? OnFrame
    {
        add
        {
            lock (StateLock)
            {
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
    /// Replaces the shared OnFrame delivery with a per-peer async sink factory.
    /// Must be called before any connection is established; each peer then
    /// gets its own frame handler.
    /// </summary>
    public void SetFrameSink(Func<IZConnection, ZFrameHandlerAsync> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        lock (StateLock)
        {
            if (peers.Count > 0)
            {
                throw new InvalidOperationException("frame sink must be set before connections are established");
            }

            frameSinkFactory = factory;
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
            if (peers.Count > 0)
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
            matches = [.. peers.Where(peer => Equals(peer.Endpoint, endpoint)).Select(peer => peer.Connection)];
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
            var targets = RouteOutbound(message, SnapshotPeers());
            foreach (var target in targets)
            {
                await WaitUntilEstablishedAsync(target, token);
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
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, owner.Memory[..bytes.Length]))));
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
            peers.Add(new Peer(connection, endpoint));
        }

        var parser = new ZmtpParser(connection, allocator, Pool);
        var sink = frameSinkFactory?.Invoke(connection) ?? BorrowedSink(parser);
        connection.SetFrameHandler((frame, _) => sink(frame, Cts.Token));

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
                ZmtpFrameEncoder.BuildHandshake(ZmtpCommands.BuildReady(SocketTypeName)),
                attemptToken);
            if (await parser.EstablishAsync(attemptToken))
            {
                var peerType = parser.PeerSocketType;
                if (peerType is null || !IsCompatibleSocketType(SocketTypeName, peerType))
                {
                    // RFC 23: on socket-type validation failure, return an
                    // ERROR command before disconnecting the peer.
                    await connection.SendCommandAsync(ZmtpCommands.BuildError("Invalid socket type"), attemptToken);
                    throw new ZeroMqProtocolException(
                        $"peer socket type '{peerType}' is not compatible with local socket type '{SocketTypeName}'");
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
            lock (StateLock)
            {
                peers.RemoveAll(peer => peer.Connection == connection);
                establishedGates.Remove(connection);
                attemptTokens.Remove(connection);
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

        if (gate is not null)
        {
            await gate.Task.WaitAsync(token);
        }
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

    private List<IZConnection> SnapshotPeers()
    {
        lock (StateLock)
        {
            return [.. peers.Select(peer => peer.Connection)];
        }
    }
}
