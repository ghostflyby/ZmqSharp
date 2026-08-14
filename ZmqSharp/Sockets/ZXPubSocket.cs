using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// XPUB composition root (0014): broadcast plus subscription observation.
/// Inbound frames (subscriptions from downstream peers) are delivered to the
/// bound sink AND forwarded to every peer except the sender, so an upstream
/// publisher learns them. Data messages broadcast to all peers (topic =
/// first frame, like PUB). The forwarding is the composed inbound policy
/// (0019); it holds a back-reference to this socket, attached after the base
/// is constructed (before any connection).
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed class ZXPubSocket : ZPubSocket
{
    public ZXPubSocket(ZSocketOptions? options = null)
        : base(options ?? new ZSocketOptions(), ZSocketTypes.XPub, new XPubInbound())
    {
        ((XPubInbound)InboundPolicy).Attach(this);
    }

    /// <summary>
    /// XPUB's inbound policy (0019): forwards an inbound subscription frame to
    /// every peer except the sender, then delivers it to the sink unchanged.
    /// </summary>
    private sealed class XPubInbound : IZInboundPolicy
    {
        private ZXPubSocket? socket;

        public void Attach(ZXPubSocket socket)
        {
            this.socket = socket;
        }

        public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            if (socket is not { } target)
                return ValueTask.FromResult(ZInboundDecision.Deliver());
            foreach (var other in target.PeerSnapshot)
            {
                if (other == peer) continue;

                // The subscription frame is a single frame whose payload is
                // 0x01/0x00 + topic; forward a per-peer fresh copy
                // (SendToAsync disposes its message exactly once).
                var payload = message[0].ToSequence().ToArray();
                var forward = new ZMessage(new ZSingleMessage(
                    new ZFrame(new ZSegment(payload, 0, payload.Length))));
                _ = target.SendToAsync(other, forward, CancellationToken.None);
            }

            return ValueTask.FromResult(ZInboundDecision.Deliver());
        }
    }
}
