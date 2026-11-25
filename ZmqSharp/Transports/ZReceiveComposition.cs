using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The post-establishment receive policy a message connection applies (0034):
/// the zero-copy allocator that materializes final frames, the encoded frame
/// bound, the message-level limits, and the per-message boundary callback that
/// resets the materializer. The runtime calls this once, after the codec is
/// known; a native message transport already owns its frames and ignores it by
/// default, but one that re-aggregates frames (ZWS) can enforce the limits
/// while reading instead of after delivery.
/// </summary>
public readonly struct ZReceiveComposition
{
    public ZReceiveComposition(
        ZFrameAllocator? allocator, long maxFrameLength, Action? onMessageBoundary,
        long maxMessageLength = long.MaxValue, int maxFramesPerMessage = int.MaxValue,
        Action? onRejected = null)
    {
        Allocator = allocator;
        MaxFrameLength = maxFrameLength;
        OnMessageBoundary = onMessageBoundary;
        MaxMessageLength = maxMessageLength;
        MaxFramesPerMessage = maxFramesPerMessage;
        OnRejected = onRejected;
    }

    /// <summary>The receive policy's final-frame allocator; null keeps staging.</summary>
    public ZFrameAllocator? Allocator { get; }

    /// <summary>The largest encoded frame the reader accepts.</summary>
    public long MaxFrameLength { get; }

    /// <summary>Runs at every complete wire-message boundary (materializer reset).</summary>
    public Action? OnMessageBoundary { get; }

    /// <summary>
    /// The largest accumulated message a native connection accepts. Byte
    /// framings enforce this through the allocator and ignore the property.
    /// </summary>
    public long MaxMessageLength { get; }

    /// <summary>
    /// The largest frame count a native connection accepts per message. Byte
    /// framings enforce this through the allocator and ignore the property.
    /// </summary>
    public int MaxFramesPerMessage { get; }

    /// <summary>
    /// Runs once when a connection enforces one of the bounds itself, so the
    /// runtime's receive-rejection counter and signal stay uniform for native
    /// connections that reject before delivery. Byte framings report through
    /// the allocator instead.
    /// </summary>
    public Action? OnRejected { get; }
}
