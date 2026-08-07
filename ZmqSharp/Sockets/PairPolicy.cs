using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>Single peer, no routing (PAIR semantics).</summary>
internal sealed class PairPolicy : IZSchedulingPolicy
{
    public IReadOnlyList<ZConnection> RouteOutbound(IZMessage message, IReadOnlyList<ZConnection> peers)
        => peers.Count == 0 ? [] : [peers[0]];

    public ZMessage OnInbound(ZMessage message, ZConnection peer) => message;
}
