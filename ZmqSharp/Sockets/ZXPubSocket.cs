using System.Buffers;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
namespace ZmqSharp;

/// <summary>
/// XPUB composition root (0014): broadcast plus subscription observation.
/// Inbound frames (subscriptions from downstream peers) are delivered to the
/// bound sink AND forwarded to every peer except the sender, so an upstream
/// publisher learns them. Data messages broadcast to all peers (topic =
/// first frame, like PUB).
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed class ZXPubSocket(ZSocketOptions options) : ZPubSocket(options, new ZXPubCore())
{
    /// <summary>
    /// Forwards an inbound subscription frame to every peer except the sender,
    /// then hands it to the sink unchanged.
    /// </summary>
    protected override ZMessage? PrepareInboundForSink(IZConnection peer, ZMessage message)
    {
        foreach (var other in PeerSnapshot)
        {
            if (other == peer) continue;

            // The subscription frame is a single frame whose payload is
            // 0x01/0x00 + topic; forward a per-peer fresh copy (SendToAsync
            // disposes its message exactly once).
            var payload = message[0].ToSequence().ToArray();
            var forward = new ZMessage(new ZSingleMessage(
                new ZFrame(new ZSegment(payload, 0, payload.Length))));
            _ = SendToAsync(other, forward, CancellationToken.None);
        }

        return message;
    }
}
