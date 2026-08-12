using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Messages;

namespace ZmqSharp.Zmtp;

/// <summary>
/// ZMTP frame encoder (the inverse of the parser): writes flags + size + body
/// per frame; a message is written atomically (never interleaved).
/// </summary>
public sealed class ZmtpFrameEncoder(Stream stream)
{
    private readonly byte[] header = new byte[9];

    /// <summary>
    /// Writes a command frame (RFC 23 short-string command body, e.g. READY).
    /// The greeting is written separately by the handshake driver
    /// (<see cref="ZmtpHandshake"/>), which also matches the mechanism.
    /// </summary>
    public async ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        long length = body.Length;
        if (length > 255)
        {
            header[0] = (byte)(ZmtpFrameFlags.Command | ZmtpFrameFlags.LongSize);
            BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1, 8), length);
            await stream.WriteAsync(header.AsMemory(0, 9), token);
        }
        else
        {
            header[0] = (byte)ZmtpFrameFlags.Command;
            header[1] = (byte)length;
            await stream.WriteAsync(header.AsMemory(0, 2), token);
        }

        await stream.WriteAsync(body, token);
    }

    public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
    {
        for (var i = 0; i < message.Count; i++)
        {
            var more = i < message.Count - 1;
            await WriteFrameAsync(message[i], more, token);
        }
    }

    private async ValueTask WriteFrameAsync(ZFrame frame, bool more, CancellationToken token)
    {
        if (frame.TryGetValue(out ZSegment segment))
        {
            await WriteFrameHeaderAsync(segment.Memory.Length, more, token);
            await stream.WriteAsync(segment.Memory, token);
            return;
        }

        if (frame.TryGetValue(out ZSegments segments))
        {
            var length = 0L;
            foreach (var seg in segments) length += seg.Memory.Length;

            await WriteFrameHeaderAsync(length, more, token);
            foreach (var seg in segments) await stream.WriteAsync(seg.Memory, token);
        }
    }

    public async ValueTask WriteFrameAsync(ReadOnlySequence<byte> frame, bool more, CancellationToken token = default)
    {
        await WriteFrameHeaderAsync(frame.Length, more, token);
        foreach (var memory in frame) await stream.WriteAsync(memory, token);
    }

    private async ValueTask WriteFrameHeaderAsync(long length, bool more, CancellationToken token)
    {
        var isLong = length > 255;
        if (isLong)
        {
            header[0] = (byte)((more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None) | ZmtpFrameFlags.LongSize);
            BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1, 8), length);
            await stream.WriteAsync(header.AsMemory(0, 9), token);
        }
        else
        {
            header[0] = (byte)(more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None);
            header[1] = (byte)length;
            await stream.WriteAsync(header.AsMemory(0, 2), token);
        }
    }
}
