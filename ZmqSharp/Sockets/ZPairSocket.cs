using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>PAIR composition root: single peer, no routing, queue receive surface (0007 section 5, 0023).</summary>
public sealed class ZPairSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZSinglePeerDispatch(), ZSocketTypes.Pair);
