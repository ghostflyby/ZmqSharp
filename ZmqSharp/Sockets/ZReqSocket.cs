using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// REQ composition root (0010 section 4): strict single in-flight request
/// over a round-robin peer selection, replies accepted only from the current
/// peer. Sends go through <see cref="RequestAsync"/>; the generic base send
/// path is rejected.
/// </summary>
public sealed class ZReqSocket : ZSocketBase, IPatternSink
{
    private readonly ZReqCore core;

    public ZReqSocket(ZSocketOptions options)
        : base(options, new ZReqCore())
    {
        core = (ZReqCore)Core;
        BindMessageSink(this);
        PeerEnded += (peer, _) => core.OnPeerEnded(peer);
    }

    /// <summary>
    /// Sends a request to the next peer (round robin) and waits for its reply.
    /// The message is consumed by the request; the returned reply is owned by
    /// the caller and disposed exactly once. Throws when a request is already
    /// in flight (strict alternation) or no peer is connected.
    /// </summary>
    public Task<ZMessage> RequestAsync(ZMessage message, CancellationToken token = default)
        => core.RequestAsync(this, message, token);

    public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token)
        => core.OnMessageAsync(this, peer, message, token);
}
