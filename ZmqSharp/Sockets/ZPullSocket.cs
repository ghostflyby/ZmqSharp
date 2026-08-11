using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>
/// PULL composition root: receive-only, fair-queue inbound. The receive
/// surface is the channel surface via <c>ZQueueSocket&lt;ZPullSocket&gt;</c>.
/// </summary>
public sealed class ZPullSocket(ZSocketOptions options) : ZSocketBase(options, new ZPullCore())
{
}
