using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// XSUB composition root (0014): manual subscription frames, no inbound
/// filter. <see cref="Subscribe"/> / <see cref="Unsubscribe"/> send the libzmq
/// wire frames (0x01/0x00 + topic) to every connected peer; every inbound
/// message reaches the bound sink.
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed class ZXSubSocket(ZSocketOptions options) : ZSubSocket(options, new ZXSubCore())
{
    /// <summary>XSUB delivers every inbound message unfiltered.</summary>
    protected override ZMessage? PrepareInboundForSink(IZConnection peer, ZMessage message)
    {
        return message;
    }
}
