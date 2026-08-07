namespace ZmqSharp.Messages;

/// <summary>
/// A single frame delivered by the low-level streaming callback. Borrowed: the
/// memory is valid only during the callback; never retained or disposed.
/// </summary>
public readonly struct ZFrame
{
    internal ZFrame(ReadOnlyMemory<byte> memory, bool more)
    {
        Memory = memory;
        More = more;
    }

    public ReadOnlyMemory<byte> Memory { get; }

    /// <summary>True when more frames of the same message follow.</summary>
    public bool More { get; }
}
