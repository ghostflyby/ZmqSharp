using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>DEALER composition root: round-robin outbound, fair-queue inbound, queue receive surface (0007 section 5, 0023).</summary>
public sealed class ZDealerSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZRoundRobinDispatch(), ZSocketTypes.Dealer);
