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

    /// <summary>Writes the 64-byte ZMTP 3.0 greeting (NULL mechanism).</summary>
    public async ValueTask WriteGreetingAsync(CancellationToken token = default)
    {
        var greeting = new byte[64];
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;
        "NULL"u8.CopyTo(greeting.AsSpan(12, 4));
        await stream.WriteAsync(greeting, token);
    }

    /// <summary>Writes a command frame (body = name + NUL + data, e.g. READY).</summary>
    public async ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        long length = body.Length;
        if (length > 255)
        {
            header[0] = 0b0110; // command + long size
            BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1, 8), length);
            await stream.WriteAsync(header.AsMemory(0, 9), token);
        }
        else
        {
            header[0] = 0b0100; // command
            header[1] = (byte)length;
            await stream.WriteAsync(header.AsMemory(0, 2), token);
        }

        await stream.WriteAsync(body, token);
    }

    public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
    {
        for (int i = 0; i < message.FrameCount; i++)
        {
            bool more = i < message.FrameCount - 1;
            await WriteFrameAsync(message.GetFrame(i), more, token);
        }
    }

    public async ValueTask WriteFrameAsync(ReadOnlySequence<byte> frame, bool more, CancellationToken token = default)
    {
        long length = frame.Length;
        bool isLong = length > 255;
        if (isLong)
        {
            header[0] = (byte)((more ? 0b0001 : 0) | 0b0010);
            BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1, 8), length);
            await stream.WriteAsync(header.AsMemory(0, 9), token);
        }
        else
        {
            header[0] = (byte)(more ? 0b0001 : 0);
            header[1] = (byte)length;
            await stream.WriteAsync(header.AsMemory(0, 2), token);
        }

        foreach (var memory in frame)
        {
            await stream.WriteAsync(memory, token);
        }
    }
}
