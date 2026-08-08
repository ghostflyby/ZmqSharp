namespace ZmqSharp.Messages;

/// <summary>
/// A single frame delivered by the low-level streaming callback. Borrowed: the
/// memory is valid only during the callback; never retained or disposed.
/// </summary>
public readonly struct ZFrame
{
    private readonly ZFrameSegments segments;

    internal ZFrame(bool more, ZFrameSegments segments)
    {
        More = more;
        this.segments = segments;
    }

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            if (segments.Single is { } single)
            {
                return single.Memory;
            }

            if (segments.Many is { Length: > 0 } many)
            {
                return many[0].Memory;
            }

            return default;
        }
    }

    /// <summary>True when more frames of the same message follow.</summary>
    public bool More { get; }

    /// <summary>Segment structure of this frame.</summary>
    internal ZFrameSegments Segments => segments;
}
