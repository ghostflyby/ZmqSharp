using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// The framing-specific half of a mechanism handshake (0033): reads and
/// writes command bodies while the mechanism session above stays
/// framing-agnostic. ZMTP carries commands as length-prefixed command frames
/// on the byte connection; ZWS carries them as single binary WebSocket
/// messages with the command flag byte. The bodies a channel returns are
/// borrowed and stay valid only until the channel's next read.
/// </summary>
internal interface IZCommandChannel : IDisposable
{
    /// <summary>Reads one command body. Null on EOF.</summary>
    ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default);

    /// <summary>Writes one command body during the exclusive handshake phase.</summary>
    ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);
}

/// <summary>
/// ZMTP command framing over a byte connection: one command frame (flag
/// byte, short or long size, body) per command, exactly as the wire contract
/// the mechanism context previously inlined.
/// </summary>
internal sealed class ZmtpCommandChannel(IZConnection connection, long maxCommandSize, MemoryPool<byte>? pool = null)
    : IZCommandChannel
{
    private readonly IZByteReader _reader = connection;
    private readonly ZmtpFrameEncoder _encoder = new(connection);
    private readonly MemoryPool<byte> _pool = pool ?? MemoryPool<byte>.Shared;
    private readonly byte[] _headerBuffer = new byte[9];
    private IMemoryOwner<byte>? _scratchOwner;
    private Memory<byte> _scratch;

    public async ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default)
    {
        if (!await TryReadExactlyAsync(_headerBuffer.AsMemory(0, 1), token)) return null;

        var flags = (ZmtpFrameFlags)_headerBuffer[0];
        if ((flags & (ZmtpFrameFlags)0b1111_1000) != 0)
            throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

        if (!flags.HasFlag(ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("expected a command frame during handshake");

        if (flags.HasFlag(ZmtpFrameFlags.More))
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var isLong = flags.HasFlag(ZmtpFrameFlags.LongSize);
        var sizeLength = isLong ? 8 : 1;
        if (!await TryReadExactlyAsync(_headerBuffer.AsMemory(1, sizeLength), token)) return null;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(_headerBuffer.AsSpan(1, 8))
            : _headerBuffer[1];
        if (size < 0) throw new ZeroMqProtocolException("negative ZMTP frame size");
        if (size > maxCommandSize)
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
        // The wire size and the configured bound are both long, so this guard
        // is live for limits above 2 GB; it protects the narrowing cast below.
        if (size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

        var length = (int)size;
        EnsureScratchCapacity(length);
        if (!await TryReadExactlyAsync(_scratch[..length], token)) return null;

        return _scratch[..length];
    }

    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => _encoder.WriteCommandAsync(body, token);

    public void Dispose()
    {
        _scratchOwner?.Dispose();
        _scratchOwner = null;
        _scratch = Memory<byte>.Empty;
    }

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await _reader.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }

    private void EnsureScratchCapacity(int required)
    {
        if (_scratch.Length >= required && _scratchOwner is not null) return;

        var newSize = Math.Max(required, Math.Max(4096, _scratch.Length * 2));
        var newOwner = _pool.Rent(newSize);
        _scratchOwner?.Dispose();
        _scratchOwner = newOwner;
        _scratch = newOwner.Memory;
    }
}
