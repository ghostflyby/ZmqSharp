using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>
/// PULL composition root: receive-only, fair-queue inbound. The receive
/// surface is the channel surface via <c>ZQueueSocket&lt;ZPullSocket&gt;</c>.
/// </summary>
public sealed class ZPullSocket(ZSocketOptions? options = null)
    : ZSocketBase(options ?? new ZSocketOptions(), new ZNoDispatch("PULL is receive-only"), ZSocketTypes.Pull);
