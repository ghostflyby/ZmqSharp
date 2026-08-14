using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>PUSH composition root: send-only, round-robin outbound (0011).</summary>
public sealed class ZPushSocket(ZSocketOptions? options = null)
    : ZSocketBase(options ?? new ZSocketOptions(), new ZRoundRobinDispatch(), ZSocketTypes.Push);
