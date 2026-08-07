using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Fair dispatch: round-robin outbound, fair-queue inbound (DEALER semantics).</summary>
internal sealed class ZDealerSocket(ZSocketOptions options) : ZSocketBase(options)
{
    private int next;

    protected override IReadOnlyList<IZConnection> RouteOutbound(
        IZMessage message,
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
