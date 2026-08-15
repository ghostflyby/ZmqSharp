using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// REQ coordination (0010 section 4; 0015 section 2.1; 0019): strict single
/// in-flight request over a fair-queue peer selection, replies accepted only
/// from the current peer. Outbound selection is split between two dispatch
/// policies: the next request target is the fair-queue selection
/// (<see cref="ZRoundRobinDispatch"/>) and the send routes through the
/// in-flight current connection (<see cref="ZCurrentPeerDispatch"/>). The
/// inbound side is the consume arm of the inbound seam (0019): the core is
/// the socket's <see cref="IZInboundPolicy"/>, completing or discarding
/// replies under the in-flight gate. Delimiter framing is
/// <see cref="ZDelimiterFraming"/>.
/// </summary>
internal sealed class ZReqCore : IZInboundPolicy
{
    private readonly Lock gateLock = new();
    private readonly ZRoundRobinDispatch fairQueue = new();
    private readonly ZCurrentPeerDispatch dispatch;

    // Scratch buffer for the fair-queue selection: requests are strictly
    // serialized by the in-flight gate, so the buffer is never used
    // concurrently and reuse keeps the request path allocation-free (0006 3.6).
    private readonly IZConnection[] singleTarget = new IZConnection[1];
    private TaskCompletionSource<ZMessage>? pending;

    internal ZReqCore(ZCurrentPeerDispatch dispatch)
    {
        this.dispatch = dispatch;
    }

    /// <summary>
    /// Sends a request to the next peer (round-robin) and returns a task that
    /// completes with the reply. Throws when a request is already in flight
    /// (strict alternation) or no peer is connected.
    /// </summary>
    public Task<ZMessage> RequestAsync(ZReqSocket socket, ZMessage message, CancellationToken token)
    {
        Task<ZMessage> result;
        lock (gateLock)
        {
            if (pending is not null) throw new InvalidOperationException("a request is already in flight");

            var peers = socket.PeerSnapshot;
            // Round robin: the cursor advances unconditionally, so a slow or
            // failing peer never starves the others (0010 section 2). The
            // selected peer becomes the current connection - the request
            // target the dispatch policy routes the send to.
            if (fairQueue.SelectTargets(message, peers, singleTarget) == 0)
                throw new InvalidOperationException("no connected peer to send the request to");

            dispatch.SetCurrent(singleTarget[0]);
            pending = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            result = pending.Task;
        }

        _ = SendFramedAsync(socket, message, token);
        return result;
    }

    /// <summary>
    /// The inbound consume arm (0019): a reply from the current peer completes
    /// the pending request; any other message is spurious and dropped.
    /// </summary>
    public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        TaskCompletionSource<ZMessage>? completion;
        lock (gateLock)
        {
            if (!dispatch.IsCurrent(peer) || pending is null)
            {
                completion = null;
            }
            else
            {
                completion = pending;
                dispatch.Clear();
                pending = null;
            }
        }

        if (completion is null)
        {
            // Not the current peer's reply (out-of-order / spurious): discard.
            message.Dispose();
            return ValueTask.FromResult(ZInboundDecision.Drop());
        }

        var reply = ZDelimiterFraming.Decode(message, "request");
        completion.TrySetResult(reply);
        return ValueTask.FromResult(ZInboundDecision.Consumed());
    }

    public void OnPeerEnded(IZConnection peer)
    {
        TaskCompletionSource<ZMessage>? completion;
        lock (gateLock)
        {
            if (!dispatch.IsCurrent(peer) || pending is null) return;

            completion = pending;
            dispatch.Clear();
            pending = null;
        }

        completion.TrySetException(new IOException("peer closed before the reply arrived"));
    }

    private async Task SendFramedAsync(ZReqSocket socket, ZMessage message, CancellationToken token)
    {
        var framed = ZDelimiterFraming.Encode(message);
        try
        {
            // The request send routes through the current-connection dispatch:
            // the selective send path asks the policy for the target, which
            // returns the in-flight request's current connection.
            await socket.SendRequestFrameAsync(framed, token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A transport-level failure (the peer retired mid-send) faults the
            // pending request and frees the in-flight gate. If the peer's
            // teardown already faulted it (OnPeerEnded), pending is null and
            // this is a no-op.
            TaskCompletionSource<ZMessage>? completion;
            lock (gateLock)
            {
                completion = pending;
                dispatch.Clear();
                pending = null;
            }

            completion?.TrySetException(ex);
        }
    }
}
