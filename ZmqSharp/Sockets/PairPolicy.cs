using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Single peer, no routing (PAIR semantics).</summary>
internal sealed class PairPolicy : IZSchedulingPolicy
{
    public IReadOnlyList<IZConnection> RouteOutbound(IZMessage message, IReadOnlyList<IZConnection> peers)
        => peers.Count == 0 ? [] : [peers[0]];

    public ZMessage? OnInbound(ZMessage message, IZConnection peer) => message;
}
