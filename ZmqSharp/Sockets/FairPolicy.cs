using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>Fair dispatch: round-robin outbound, fair-queue inbound (DEALER semantics).</summary>
internal sealed class FairPolicy : IZSchedulingPolicy
{
    private int next;

    public IReadOnlyList<ZConnection> RouteOutbound(ZMessage message, IReadOnlyList<ZConnection> peers)
    {
        if (peers.Count == 0)
        {
            return [];
        }

        int index = (int)(Interlocked.Increment(ref next) - 1) % peers.Count;
        return [peers[index]];
    }

    public ZMessage? OnInbound(ZMessage message, ZConnection peer) => message;
}
