using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Single peer, no routing (PAIR semantics).</summary>
public sealed class ZPairSocket(ZSocketOptions options) : ZSocketBase(options)
{
    protected override IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
        => peers.IsEmpty ? null : peers[0];

    protected override string SocketTypeName => "PAIR";
}
