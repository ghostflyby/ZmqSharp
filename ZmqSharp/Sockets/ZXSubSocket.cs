using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>
/// XSUB composition root (0014): manual subscription frames, no inbound
/// filter. <see cref="Subscribe"/> / <see cref="Unsubscribe"/> send the libzmq
/// wire frames (0x01/0x00 + topic) to every connected peer; every inbound
/// message reaches the bound sink (pass-through inbound, unlike SUB).
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed class ZXSubSocket(ZSocketOptions? options = null) : ZSubSocket(options ?? new ZSocketOptions(), ZSocketTypes.XSub)
{
}
