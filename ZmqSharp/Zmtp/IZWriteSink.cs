using System.Buffers;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Write target of the ZMTP frame encoder (0015 section 6.1): each frame is
/// produced as one <see cref="ReadOnlySequence{T}"/> and handed to the sink in
/// a single logical write, preserving the frame's segment structure. The
/// socket sink uses buffer-list scatter writes (one system call per frame);
/// the stream sink writes the segments sequentially (the previous behavior).
/// </summary>
internal interface IZWriteSink
{
    ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);

    ValueTask WriteAsync(ReadOnlySequence<byte> sequence, CancellationToken token = default);
}
