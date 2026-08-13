using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>
/// REQ composition root (0010 section 4; 0019): strict single in-flight
/// request over a round-robin peer selection, replies accepted only from the
/// current peer. The current connection is owned by the composed
/// <see cref="ZCurrentPeerDispatch"/>; the request send routes through it and
/// the reply intake is the consume arm of the composed inbound policy (the
/// <see cref="ZReqCore"/>), so a bound <see cref="BindMessageSink"/> consumer
/// is never hijacked by the protocol. Sends go through
/// <see cref="RequestAsync"/>; the generic base send path is rejected when no
/// request is in flight.
/// </summary>
public sealed class ZReqSocket : ZSocketBase
{
    private readonly ZReqCore core;

    public ZReqSocket(ZSocketOptions options)
        : this(options, new ZCurrentPeerDispatch())
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
}
