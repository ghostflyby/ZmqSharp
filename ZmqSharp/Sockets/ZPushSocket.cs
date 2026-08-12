
using ZmqSharp.Sockets;
namespace ZmqSharp;

/// <summary>PUSH composition root: send-only, round-robin outbound (0011).</summary>
public sealed class ZPushSocket(ZSocketOptions options) : ZSocketBase(options, new ZPushCore());
