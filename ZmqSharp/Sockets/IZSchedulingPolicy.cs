using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// The only per-socket-type difference: how messages are scheduled to peers.
/// </summary>
internal interface IZSchedulingPolicy
{
    /// <summary>Select the outbound connection(s) for a message; empty = drop.</summary>
    IReadOnlyList<IZConnection> RouteOutbound(IZMessage message, IReadOnlyList<IZConnection> peers);

    /// <summary>Transform or drop an inbound message from a peer; null = drop.</summary>
    ZMessage? OnInbound(ZMessage message, IZConnection peer);
}
