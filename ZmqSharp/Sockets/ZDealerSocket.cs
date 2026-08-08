using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Fair dispatch: round-robin outbound, fair-queue inbound (DEALER semantics).</summary>
public sealed class ZDealerSocket(ZSocketOptions options) : ZSocketBase(options)
{
    private int next;

    protected override IReadOnlyList<IZConnection> RouteOutbound(
        ZMessage message,
        IReadOnlyList<IZConnection> peers)
    {
        if (peers.Count == 0)
        {
            return [];
        }

        int index = (Interlocked.Increment(ref next) - 1) % peers.Count;
        return [peers[index]];
    }
}
