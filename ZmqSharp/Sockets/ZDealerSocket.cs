using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>DEALER composition root: round-robin outbound, fair-queue inbound (0007 section 5).</summary>
public sealed class ZDealerSocket(ZSocketOptions options)
    : ZSocketBase(options, new ZRoundRobinDispatch(), ZSocketTypes.Dealer);
