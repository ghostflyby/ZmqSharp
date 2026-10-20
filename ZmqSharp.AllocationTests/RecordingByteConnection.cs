using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.AllocationTests;

internal sealed class RecordingByteConnection(byte[]? feed = null, int capacity = 0, bool yieldWrites = false) : IZConnection
{
    private readonly byte[] _feed = feed ?? [];
    private readonly List<byte> _written = new(capacity);
    private int _position;
    public byte[] Recorded => [.. _written];

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var length = Math.Min(destination.Length, _feed.Length - _position);
        _feed.AsMemory(_position, length).CopyTo(destination);
        _position += length;
        return ValueTask.FromResult(length);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        if (yieldWrites) await Task.Yield();
        token.ThrowIfCancellationRequested();
        _written.AddRange(bytes.Span);
    }

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        foreach (var segment in bytes) await WriteAsync(segment, token);
    }

    public void Abort() { }
    public void Dispose() { }
}
