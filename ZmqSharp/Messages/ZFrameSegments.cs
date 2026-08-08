namespace ZmqSharp.Messages;

/// <summary>
/// Segment expression for one frame: a single segment (no array) or a segment
/// table. Keeps the common single-segment case allocation-free.
/// </summary>
internal readonly struct ZFrameSegments
{
    /// <summary>Single-segment frame; null when the frame is segmented.</summary>
    public ZBufferRef? Single { get; init; }

    /// <summary>Segment table of a segmented frame; null for single-segment frames.</summary>
    public ZBufferRef[]? Many { get; init; }
}
