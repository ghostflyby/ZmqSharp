using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Common contract for assembled messages: frame count, per-frame access, and
/// the whole payload. A frame is contiguous unless it spans several segments.
/// </summary>
public interface IZMessage : IDisposable
{
    int FrameCount { get; }

    ReadOnlySequence<byte> GetFrame(int index);

    bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory);

    ReadOnlySequence<byte> Payload { get; }
}
