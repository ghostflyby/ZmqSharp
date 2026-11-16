using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The post-establishment receive policy a message connection applies (0034):
/// the zero-copy allocator that materializes final frames, the encoded frame
/// bound, and the per-message boundary callback that resets the materializer.
/// The runtime calls this once, after the codec is known; a native message
/// transport already owns its frames and ignores it by default.
/// </summary>
public readonly struct ZReceiveComposition
{
    public ZReceiveComposition(
        ZFrameAllocator? allocator, long maxFrameLength, Action? onMessageBoundary)
    {
        Allocator = allocator;
        MaxFrameLength = maxFrameLength;
        OnMessageBoundary = onMessageBoundary;
    }

    /// <summary>The receive policy's final-frame allocator; null keeps staging.</summary>
    public ZFrameAllocator? Allocator { get; }

    /// <summary>The largest encoded frame the reader accepts.</summary>
    public long MaxFrameLength { get; }

    /// <summary>Runs at every complete wire-message boundary (materializer reset).</summary>
    public Action? OnMessageBoundary { get; }
}
