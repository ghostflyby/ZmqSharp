using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>A protocol-independent byte connection over a stream.</summary>
internal sealed class ZConnection(Stream stream, in ZTransportContext context)
    : ZByteConnection(context)
{
    private int _aborted;

    public ZConnection(Stream stream) : this(stream, ZTransportContext.Default)
    {
    }

    public override ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        => stream.ReadAsync(destination, token);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => stream.WriteAsync(bytes, token);

    public override async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        foreach (var segment in bytes) await stream.WriteAsync(segment, token);
    }

    public override void Abort()
    {
        if (Interlocked.Exchange(ref _aborted, 1) == 0) stream.Dispose();
    }

    protected override void DisposeCore() => Abort();
}
