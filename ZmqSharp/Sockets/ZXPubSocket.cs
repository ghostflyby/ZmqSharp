using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>Broadcast publishing with subscription observation and tracked subscription forwarding.</summary>
// ReSharper disable once InconsistentNaming
public sealed class ZXPubSocket(ZSocketOptions? options = null) : ZPubSocket(Create(options ?? new ZSocketOptions()))
{
    private static SocketRuntime Create(ZSocketOptions options)
    {
        var runtime = new SocketRuntime(options, new ZBroadcastDispatch(), ZSocketTypes.XPub, supportsQueue: true);
        runtime.ConfigureInbound(new XPubCoordinator(() => runtime.PeerSnapshot, runtime.SendTracked));
        return runtime;
    }
}
