using ZmqSharp.Patterns;

namespace ZmqSharp.Sockets;

/// <summary>Coordinates one request at a time without depending on a socket implementation.</summary>
internal sealed class ZReqCore : IZInboundPolicy
{
    private sealed class Request(ZPeer peer, CancellationToken token)
    {
        private int finished;
        public bool TryFinish() => Interlocked.CompareExchange(ref finished, 1, 0) == 0;
        public ZPeer Peer { get; } = peer;
        public CancellationToken Token { get; } = token;
        public TaskCompletionSource<ZMessage> Outcome { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<ZMessage> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly Lock gate = new();
    private readonly ZRoundRobinDispatch selection = new();
    private readonly ZPeer[] target = new ZPeer[1];
    private readonly Func<ZPeer[]> peers;
    private readonly Func<ZPeer, ZMessage, CancellationToken, ValueTask> send;
    private readonly Action<ZPeer> retire;
    private Request? pending;

    public ZReqCore(Func<ZPeer[]> peers,
        Func<ZPeer, ZMessage, CancellationToken, ValueTask> send, Action<ZPeer> retire)
    {
        this.peers = peers;
        this.send = send;
        this.retire = retire;
    }

    /// <summary>Transfers the message only after preconditions succeed; completion ends all buffer borrowing.</summary>
    public Task<ZMessage> RequestAsync(ZMessage message, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        Request request;
        lock (gate)
        {
            if (pending is not null) throw new InvalidOperationException("a request is already in flight");
            if (selection.SelectTargets(message, peers(), target) == 0)
                throw new InvalidOperationException("no connected peer to send the request to");
            request = new Request(target[0], token);
            pending = request;
        }

        _ = RunAsync(request, message);
        return request.Completion.Task;
    }

    private async Task RunAsync(Request request, ZMessage message)
    {
        // Register before sending so cancellation also covers a send that never starts.
        var registration = request.Token.UnsafeRegister(_ => Cancel(request), null);
        ZMessage reply = default;
        var ownsReply = false;
        Exception? failure = null;
        var outbound = message;
        try
        {
            request.Token.ThrowIfCancellationRequested();
            outbound = ZDelimiterFraming.Encode(message);
            await send(request.Peer, outbound, request.Token);
        }
        catch (Exception ex)
        {
            if (ex is OperationCanceledException) Cancel(request);
            else Fail(request, ex, true);
        }
        finally
        {
            outbound.Dispose();
        }

        try
        {
            reply = await request.Outcome.Task;
            ownsReply = true;
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        await registration.DisposeAsync();

        // The send and cancellation callback have finished before the public task or slot is released.
        lock (gate)
        {
            if (ReferenceEquals(pending, request)) pending = null;
        }

        if (ownsReply) request.Completion.TrySetResult(reply);
        else if (failure is OperationCanceledException) request.Completion.TrySetCanceled(request.Token);
        else if (failure is { } error) request.Completion.TrySetException(error);
    }

    private void Cancel(Request request)
    {
        if (!request.TryFinish()) return;
        // Retire before publishing the outcome: old replies cannot reach a new request.
        retire(request.Peer);
        request.Outcome.TrySetCanceled(request.Token);
    }

    private void Fail(Request request, Exception error, bool retirePeer)
    {
        if (!request.TryFinish()) return;
        if (retirePeer) retire(request.Peer);
        request.Outcome.TrySetException(error);
    }

    public ValueTask<ZInboundDecision> DecideAsync(ZPeer peer, ZMessage message, CancellationToken token)
    {
        Request? request;
        lock (gate) request = pending is { } active && ReferenceEquals(active.Peer, peer) ? active : null;
        if (request is null)
        {
            message.Dispose();
            return ValueTask.FromResult(ZInboundDecision.Drop());
        }

        ZMessage reply;
        try
        {
            reply = ZDelimiterFraming.Decode(message, "reply");
        }
        catch (Exception ex)
        {
            Fail(request, ex, true);
            throw;
        }

        if (request.TryFinish()) request.Outcome.TrySetResult(reply);
        else reply.Dispose();
        return ValueTask.FromResult(ZInboundDecision.Consumed());
    }

    public void OnPeerEnded(ZPeer peer)
    {
        lock (gate)
        {
            if (pending is { } request && ReferenceEquals(request.Peer, peer))
                Fail(request, new IOException("peer closed before the reply arrived"), false);
        }
    }
}
