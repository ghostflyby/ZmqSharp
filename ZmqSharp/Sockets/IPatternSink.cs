using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// Semantic delivery seam (0007 section 2.3): complete messages, per peer,
/// serialized. A completed task continues that peer's delivery; a pending
/// task pauses its pump until it completes. Ownership of the message
/// transfers to the surface, which disposes it exactly once.
/// </summary>
public interface IPatternSink
{
    /// <summary>Delivers one complete message from <paramref name="peer"/>.</summary>
    ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default);
}
