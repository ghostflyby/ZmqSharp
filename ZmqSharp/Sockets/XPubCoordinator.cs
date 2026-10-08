using System.Buffers;
using ZmqSharp.Patterns;

namespace ZmqSharp.Sockets;

internal sealed class XPubCoordinator(Func<ZPeer[]> peers, Action<ZPeer, ZMessage> forward) : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(ZPeer peer, ZMessage message, CancellationToken token)
    {
        foreach (var other in peers())
        {
            if (ReferenceEquals(peer, other)) continue;
            forward(other, ZMessage.FromOwned(message[0].ToSequence().ToArray()));
        }
        return ValueTask.FromResult(ZInboundDecision.Deliver());
    }
}
