namespace ZmqSharp.Transports;

/// <summary>Single-reader asynchronous byte input. Zero bytes means EOF.</summary>
public interface IZByteReader
{
    ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default);
}
