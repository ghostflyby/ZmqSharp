using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>
/// The only per-socket-type difference: how messages are scheduled to peers.
/// </summary>
internal interface IZSchedulingPolicy
{
    /// <summary>Select the outbound connection(s) for a message; empty = drop.</summary>
    IReadOnlyList<ZConnection> RouteOutbound(IZMessage message, IReadOnlyList<ZConnection> peers);

    /// <summary>Transform or drop an inbound message from a peer; null = drop.</summary>
    ZMessage? OnInbound(ZMessage message, ZConnection peer);
}
