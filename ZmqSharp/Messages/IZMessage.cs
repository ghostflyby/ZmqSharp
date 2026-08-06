using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Read-only view of a message: preserves multipart frame boundaries, and a
/// frame may be contiguous or segmented. Ownership is defined by the concrete
/// type (ZMessageView borrows, ZMessage owns).
/// </summary>
public interface IZMessage
{
    /// <summary>Number of frames (at least 1 for a delivered message).</summary>
    int FrameCount { get; }

    /// <summary>Returns the byte sequence of frame <paramref name="index"/> without copying.</summary>
    ReadOnlySequence<byte> GetFrame(int index);

    /// <summary>
    /// If frame <paramref name="index"/> lies in a single segment, returns its
    /// ReadOnlyMemory; otherwise returns false.
    /// </summary>
    bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory);

    /// <summary>Concatenated view of all frames (equals the frame for single-frame messages).</summary>
    ReadOnlySequence<byte> Whole { get; }
}
