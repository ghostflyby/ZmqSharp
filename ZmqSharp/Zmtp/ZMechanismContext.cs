using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Connection-scoped wire view a mechanism session drives on (0016 section 3).
/// The context lives only for the duration of <see cref="IZMechanismSession.RunAsync"/>;
/// the driver disposes it when the session returns, so a session must not
/// retain the context or the command memories it hands out. Commands are read
/// into a reusable scratch rented from the shared pool; the returned
/// <see cref="ZMechanismCommand"/> memories are borrowed and stay valid only
/// until the next read on this context.
/// </summary>
public sealed class ZMechanismContext : IDisposable
{
    private readonly int maxCommandSize;
    private readonly MemoryPool<byte> pool;
    private readonly byte[] headerBuffer = new byte[9];
    private IMemoryOwner<byte>? scratchOwner;
    private Memory<byte> scratch;

    internal ZMechanismContext(
        IZConnection connection,
        ReadOnlyMemory<byte> localReadyBody,
        int maxCommandSize,
        MemoryPool<byte>? pool = null)
    {
        Connection = connection;
        LocalReadyBody = localReadyBody;
        this.maxCommandSize = maxCommandSize;
        this.pool = pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>The raw connection; also the session connection for cleartext mechanisms.</summary>
    public IZConnection Connection { get; }

    /// <summary>
    /// Local READY body built by the socket layer; the session sends it at the
    /// protocol-correct point of its command sequence (NULL: immediately;
    /// PLAIN: after WELCOME).
    /// </summary>
    public ReadOnlyMemory<byte> LocalReadyBody { get; }

    /// <summary>Command-frame size limit shared with the traffic parser (0008 Slice B).</summary>
    public int MaxCommandSize => maxCommandSize;

    /// <summary>Writes one command frame (header + body) under the connection write gate.</summary>
    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return Connection.SendCommandAsync(body, token);
    }

    /// <summary>
    /// Reads one command frame. The returned command's memories are borrowed
    /// from this context's scratch and stay valid until the next read; a
    /// session that must retain them copies them. Returns null on EOF.
    /// </summary>
    public async ValueTask<ZMechanismCommand?> ReadCommandAsync(CancellationToken token = default)
    {
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(0, 1), token)) return null;

        var flags = (ZmtpFrameFlags)headerBuffer[0];
        if ((flags & (ZmtpFrameFlags)0b1111_1000) != 0)
            throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

        if (!flags.HasFlag(ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("expected a command frame during handshake");

        if (flags.HasFlag(ZmtpFrameFlags.More))
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var isLong = flags.HasFlag(ZmtpFrameFlags.LongSize);
        var sizeLength = isLong ? 8 : 1;
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(1, sizeLength), token)) return null;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(headerBuffer.AsSpan(1, 8))
            : headerBuffer[1];
        if (size < 0) throw new ZeroMqProtocolException("negative ZMTP frame size");
        if (size > maxCommandSize)
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
        if (size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

        var length = (int)size;
        EnsureScratchCapacity(length);
        if (!await TryReadExactlyAsync(scratch[..length], token)) return null;

        var body = scratch[..length].Span;
        if (!ZmtpCommandCodec.TryReadCommandName(body, out var name))
            throw new ZeroMqProtocolException("malformed command name");

        return new ZMechanismCommand(
            scratch.Slice(1, name.Length),
            scratch[(1 + name.Length)..length]);
    }

    public void Dispose()
    {
        scratchOwner?.Dispose();
        scratchOwner = null;
        scratch = Memory<byte>.Empty;
    }

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await Connection.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }

    private void EnsureScratchCapacity(int required)
    {
        if (scratch.Length >= required) return;

        var newSize = Math.Max(required, Math.Max(4096, scratch.Length * 2));
        var newOwner = pool.Rent(newSize);
        scratchOwner?.Dispose();
        scratchOwner = newOwner;
        scratch = newOwner.Memory;
    }
}
