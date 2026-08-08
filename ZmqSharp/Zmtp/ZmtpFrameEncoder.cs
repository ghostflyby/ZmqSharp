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

    /// <summary>ZMTP 3.0 greeting with the NULL mechanism; fixed wire constant.</summary>
    internal static readonly byte[] NullGreeting = BuildNullGreeting();

    /// <summary>Writes the 64-byte ZMTP 3.0 greeting (NULL mechanism).</summary>
    public async ValueTask WriteGreetingAsync(CancellationToken token = default)
    {
        await stream.WriteAsync(NullGreeting, token);
    }

    /// <summary>
    /// Builds the greeting plus one command frame as a single buffer, so the
    /// handshake can be written atomically and a concurrent data frame cannot
    /// interleave and corrupt the peer's handshake.
    /// </summary>
    internal static byte[] BuildHandshake(ReadOnlyMemory<byte> commandBody)
    {
        var handshake = new byte[NullGreeting.Length + 2 + commandBody.Length];
        NullGreeting.CopyTo(handshake);
        handshake[NullGreeting.Length] = (byte)ZmtpFrameFlags.Command;
        handshake[NullGreeting.Length + 1] = (byte)commandBody.Length;
        commandBody.Span.CopyTo(handshake.AsSpan(NullGreeting.Length + 2));
        return handshake;
    }

    /// <summary>Writes a command frame (body = name + NUL + data, e.g. READY).</summary>
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
        for (int i = 0; i < message.Count; i++)
        {
            bool more = i < message.Count - 1;
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
            foreach (var seg in segments)
            {
                length += seg.Memory.Length;
            }

            await WriteFrameHeaderAsync(length, more, token);
            foreach (var seg in segments)
            {
                await stream.WriteAsync(seg.Memory, token);
            }
        }
    }

    public async ValueTask WriteFrameAsync(ReadOnlySequence<byte> frame, bool more, CancellationToken token = default)
    {
        await WriteFrameHeaderAsync(frame.Length, more, token);
        foreach (var memory in frame)
        {
            await stream.WriteAsync(memory, token);
        }
    }

    private async ValueTask WriteFrameHeaderAsync(long length, bool more, CancellationToken token)
    {
        bool isLong = length > 255;
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

    private static byte[] BuildNullGreeting()
    {
        var greeting = new byte[64];
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;
        "NULL"u8.CopyTo(greeting.AsSpan(12, 4));
        return greeting;
    }
}
