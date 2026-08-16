using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>
/// REQ composition root (0010 section 4; 0019): strict single in-flight
/// request over a round-robin peer selection, replies accepted only from the
/// current peer. The current connection is owned by the composed
/// <see cref="ZCurrentPeerDispatch"/>; the request send routes through it and
/// the reply intake is the consume arm of the composed inbound policy (the
/// <see cref="ZReqCore"/>), so a <see cref="ZSocketOptions.MessageSink"/>
/// consumer is never hijacked by the protocol. Sends go through
/// <see cref="RequestAsync"/>; the generic base send path is rejected when no
/// request is in flight.
/// </summary>
public sealed class ZReqSocket : ZSocketBase
{
    private readonly ZReqCore core;

    public ZReqSocket(ZSocketOptions? options = null)
        : this(options ?? new ZSocketOptions(), new ZCurrentPeerDispatch())
    {
    }

    private ZReqSocket(ZSocketOptions options, ZCurrentPeerDispatch dispatch)
        : base(options, dispatch, ZSocketTypes.Req, new ZReqCore(dispatch))
    {
        core = (ZReqCore)InboundPolicy;
        PeerEnded += (peer, _) => core.OnPeerEnded(peer);
    }

    /// <summary>
    /// Sends a request to the next peer (round-robin) and waits for its reply.
    /// The message is consumed by the request; the returned reply is owned by
    /// the caller and disposed exactly once. Throws when a request is already
    /// in flight (strict alternation) or no peer is connected.
    /// </summary>
    public Task<ZMessage> RequestAsync(ZMessage message, CancellationToken token = default)
    {
        return core.RequestAsync(this, message, token);
    }

    /// <summary>
    /// Sends a request that borrows the caller's buffer instead of copying
    /// (0026 3.6): zero-copy, no pool rent, for any
    /// <see cref="ReadOnlyMemory{T}"/> backing. The caller must not modify
    /// the buffer until the reply arrives (the request is consumed only after
    /// the reply, by protocol causality); a synchronous throw (no peer, a
    /// request in flight) ends the borrow immediately.
    /// </summary>
    public Task<ZMessage> RequestAsync(ReadOnlyMemory<byte> request, CancellationToken token = default)
    {
        var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed(request))));
        try
        {
            return core.RequestAsync(this, message, token);
        }
        catch
        {
            // Synchronous throw (no peer, in-flight): nothing owns the message.
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a request with non-contiguous content, copied, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(ReadOnlySequence<byte> request, CancellationToken token = default)
    {
        var message = ZMessage.Copy(request);
        try
        {
            return core.RequestAsync(this, message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a multipart request, copied frame by frame, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        var message = ZMessage.Copy(frames);
        try
        {
            return core.RequestAsync(this, message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a multipart request from a <c>byte[][]</c> collection, copied, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(IEnumerable<byte[]> frames, CancellationToken token = default)
    {
        var message = ZMessage.Copy(frames);
        try
        {
            return core.RequestAsync(this, message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Internal frame send for the REQ core (0024): routes the framed request
    /// through the current-connection dispatch. REQ exposes no public generic
    /// SendAsync - sends go through <see cref="RequestAsync"/>.
    /// </summary>
    internal ValueTask SendRequestFrameAsync(ZMessage framed, CancellationToken token)
    {
        return SendAsyncCore(framed, token);
    }
}
