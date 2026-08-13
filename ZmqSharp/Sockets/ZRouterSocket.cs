using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// ROUTER composition root (0012): identity-aware routing. Inbound messages
/// arrive with the peer's routing identity prefixed as the first frame
/// (consumers bind their own <see cref="IPatternSink"/> or wrap this in the
/// channel surface); <see cref="SendAsync(byte[], ZMessage, CancellationToken)"/>
/// addresses a peer by identity. The routing table and the inbound identity
/// prefix both live in the composed <see cref="ZIdentityDispatch"/> / its
/// inbound policy; this socket only delegates. Wire frames carry no identity
/// (ZMTP 3.0 routing ids are local to the router).
/// </summary>
public sealed class ZRouterSocket : ZSocketBase
{
    private readonly ZIdentityDispatch dispatch;

    public ZRouterSocket(ZSocketOptions options)
        : this(options, new ZIdentityDispatch())
    {
    }

    private ZRouterSocket(ZSocketOptions options, ZIdentityDispatch dispatch)
        : base(options, dispatch, ZSocketTypes.Router, new RouterInboundPolicy(dispatch))
    {
        this.dispatch = dispatch;
        // The identity mapping lives in the routing policy; release it on
        // teardown so a long-lived ROUTER never retains disposed connections
        // or stale ids (subagent review finding).
        PeerEnded += (peer, _) => dispatch.RemovePeer(peer);
    }

    /// <summary>
    /// Sends to the peer identified by <paramref name="identity"/>; an unknown
    /// identity drops the message (libzmq ROUTER default). The message is
    /// disposed after the send.
    /// </summary>
    public async ValueTask SendAsync(byte[] identity, ZMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(identity);
        if (!dispatch.TryResolve(identity, out var peer) || peer is null)
        {
            message.Dispose();
            return;
        }

        await SendToAsync(peer, message, token);
    }

    public async ValueTask SendAsync(byte[] identity, ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, bytes.Length))));
        await SendAsync(identity, message, token);
    }

    /// <summary>
    /// ROUTER's inbound policy (0019): assigns the peer its routing identity
    /// through the routing dispatch and delivers the message prefixed with it.
    /// </summary>
    private sealed class RouterInboundPolicy(ZIdentityDispatch dispatch) : IZInboundPolicy
    {
        public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            var identity = dispatch.AssignIdentity(peer);
            var frames = new List<ZFrame>(message.Count + 1)
            {
                new(new ZSegment(identity, 0, identity.Length))
            };
            for (var i = 0; i < message.Count; i++) frames.Add(message[i]);

            return ValueTask.FromResult(ZInboundDecision.Deliver(
                new ZMessage(new ZMultiMessage([.. frames]))));
        }
    }
}
