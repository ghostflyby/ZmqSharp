using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Common contract for assembled messages: a read-only list of frames with a
/// zero-copy contiguous-frame probe. A frame is contiguous unless it spans
/// several segments.
/// </summary>
public interface IZMessage : IReadOnlyList<ReadOnlySequence<byte>>, IDisposable
{
    bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory);

    /// <summary>
    /// Gets the backing array of an owned, single-segment frame without
    /// copying. Returns false for pooled frames and for segmented frames.
    /// The array may be longer than the frame's content.
    /// </summary>
    bool TryGetOwnedArray(int index, out byte[] array);
}
