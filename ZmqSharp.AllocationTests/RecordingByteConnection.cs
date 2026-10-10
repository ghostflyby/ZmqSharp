using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.AllocationTests;

internal sealed class RecordingByteConnection(byte[]? feed = null, int capacity = 0, bool yieldWrites = false) : IZConnection
{
    private readonly byte[] feed = feed ?? [];
    private readonly List<byte> written = new(capacity);
    private int position;
    public byte[] Recorded => [.. written];

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var length = Math.Min(destination.Length, feed.Length - position);
        feed.AsMemory(position, length).CopyTo(destination);
        position += length;
        return ValueTask.FromResult(length);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        if (yieldWrites) await Task.Yield();
        token.ThrowIfCancellationRequested();
        written.AddRange(bytes.Span);
    }

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        foreach (var segment in bytes) await WriteAsync(segment, token);
    }

    public void Abort() { }
    public void Dispose() { }
}
