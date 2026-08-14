using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>
/// PULL composition root: receive-only, fair-queue inbound. The receive
/// surface is the default queue surface (0023): messages are read through
/// <see cref="ZQueueSocketBase.Messages"/>.
/// </summary>
public sealed class ZPullSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZNoDispatch("PULL is receive-only"), ZSocketTypes.Pull);
