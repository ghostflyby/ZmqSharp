using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>A protocol-independent byte connection over a stream.</summary>
internal sealed class ZConnection(Stream stream) : IZConnection
{
    private int aborted;

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        => stream.ReadAsync(destination, token);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => stream.WriteAsync(bytes, token);

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        foreach (var segment in bytes) await stream.WriteAsync(segment, token);
    }

    public void Abort()
    {
        if (Interlocked.Exchange(ref aborted, 1) == 0) stream.Dispose();
    }

    public void Dispose() => Abort();
}
