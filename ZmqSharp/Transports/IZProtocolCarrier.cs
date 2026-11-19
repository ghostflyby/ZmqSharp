using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// A connection that brings its own wire protocol (0036): the transport
/// negotiated it with the peer, so the runtime drives the connection through
/// this protocol instead of the socket-configured default. The configured
/// protocol still wins when the socket explicitly sets
/// <see cref="ZSocketOptions.WireProtocol"/>.
/// </summary>
internal interface IZProtocolCarrier
{
    /// <summary>The wire protocol this connection speaks.</summary>
    IZWireProtocol Protocol { get; }
}
