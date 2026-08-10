using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Fair dispatch: round-robin outbound, fair-queue inbound (DEALER semantics).</summary>
public sealed class ZDealerSocket(ZSocketOptions options) : ZSocketBase(options)
{
    private int next;

    protected override IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
    {
        if (peers.IsEmpty)
        {
            return null;
        }

        int index = (Interlocked.Increment(ref next) - 1) % peers.Length;
        return peers[index];
    }

    protected override string SocketTypeName => "DEALER";
}
