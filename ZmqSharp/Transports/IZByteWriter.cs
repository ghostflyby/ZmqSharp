using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>Single-writer byte output. Completion means the entire input was written.</summary>
public interface IZByteWriter
{
    ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default);
}
