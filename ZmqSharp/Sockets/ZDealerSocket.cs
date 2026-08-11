namespace ZmqSharp.Sockets;

/// <summary>DEALER composition root: round-robin outbound, fair-queue inbound (0007 section 5).</summary>
public sealed class ZDealerSocket(ZSocketOptions options) : ZSocketBase(options, new ZDealerCore());
