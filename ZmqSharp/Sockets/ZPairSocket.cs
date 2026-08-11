namespace ZmqSharp.Sockets;

/// <summary>PAIR composition root: single peer, no routing (0007 section 5).</summary>
public sealed class ZPairSocket(ZSocketOptions options) : ZSocketBase(options, new ZPairCore());
