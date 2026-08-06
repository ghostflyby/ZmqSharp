using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipelines;

namespace ZmqSharp;

public struct ZLinkedMessage : IZMessage
{
    public required ReadOnlySequence<byte> Sequence { get; init; }
}

public interface IZMessage
{
    public ReadOnlySequence<byte> Sequence { get; }
}

public readonly struct ZMessage(IMemoryOwner<byte> memoryOwner) : IZMessage, IMemoryOwner<byte>
{
    public ReadOnlySequence<byte> Sequence => new(Memory);

    public void Dispose()
    {
        memoryOwner.Dispose();
    }

    public Memory<byte> Memory => memoryOwner.Memory;
}

// ReSharper disable once InconsistentNaming
public class ZeroMQProtocolException : InvalidOperationException;

// ReSharper disable once InconsistentNaming
public static class ZeroMQ
{
    public static void ParseZmtp(PipeReader reader, Action<ZLinkedMessage> action)
    {
        while (true)
        {
            reader.ParseConnection();
        }

        // Mark the PipeReader as complete.
        reader.Complete();
    }

    private static void ParseConnection(this PipeReader reader)
    {
        if (!reader.ParseGreeting() ||
            !reader.ParseHandshake())
        {
            reader.Complete(new ZeroMQProtocolException());
            return false;
        }
    }

    private static bool ParseGreeting(this PipeReader reader)
    {
        Span<byte> buffer = stackalloc byte[64];
        if (!reader.TryReadExact(buffer))
        {
            reader.Complete(new ZeroMQProtocolException());
            return false;
        }

        var signature = buffer[..10];
        var version = buffer[10..12];
        var mechanism = buffer[12..32];
        var asServer = buffer[32];
        var filler = buffer[33..];


        return true;
    }

    private static bool ParseMessages(this PipeReader reader, Action<ZLinkedMessage> action)
    {
        Span<byte> sizeBuffer = stackalloc byte[4];
        while (reader.TryReadByte(out var firstByte))
        {
            var flags = (ZmtpFrameFlags)firstByte;
            if (!(flags.HasFlag(ZmtpFrameFlags.Last) || flags.HasFlag(ZmtpFrameFlags.More)) || !reader.TryReadExact(
                    flags.HasFlag(ZmtpFrameFlags.LongSize) ? sizeBuffer : sizeBuffer[..1])
               )
            {
                reader.Complete(new ZeroMQProtocolException());
                return false;
            }

            var size = BinaryPrimitives.ReadInt64BigEndian(sizeBuffer);
        }
    }

    private static bool ParseHandshake(this PipeReader reader)
    {
        while (reader.ParseCommand())
        {
        }

        return true;
    }

    extension(PipeReader reader)
    {
        private bool TryReadExact(Span<byte> span)
        {
            while (reader.TryRead(out var result))
            {
                var buffer = result.Buffer;
                if (buffer.Length < span.Length)
                {
                    if (result.IsCompleted)
                        return false;
                    continue;
                }

                var toRead = buffer.Slice(0, span.Length);
                toRead.CopyTo(span);
                reader.AdvanceTo(toRead.End);
                break;
            }

            return true;
        }

        private bool TryReadByte(out byte result)
        {
            while (reader.TryRead(out var readResult))
            {
                var buffer = readResult.Buffer;
                if (buffer.IsEmpty)
                {
                    if (readResult.IsCompleted)
                    {
                        result = 0;
                        return false;
                    }

                    continue;
                }


                result = buffer.FirstSpan[0];
                reader.AdvanceTo(buffer.GetPosition(1));
                return true;
            }

            throw new UnreachableException();
        }

        /// <returns>has more command to parse</returns>
        private bool ParseCommand()
        {
            throw new NotImplementedException();
        }
    }
}