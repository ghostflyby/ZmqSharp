namespace ZmqSharp.Zmtp;

/// <summary>
/// Byte source (transport seam): semantics match Stream.ReadAsync; returning 0 signals EOF.
/// </summary>
public interface IZByteSource
{
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default);
}
