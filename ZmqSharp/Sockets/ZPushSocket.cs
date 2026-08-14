using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>PUSH composition root: send-only, round-robin outbound (0011, 0023).</summary>
public sealed class ZPushSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZRoundRobinDispatch(), ZSocketTypes.Push);
