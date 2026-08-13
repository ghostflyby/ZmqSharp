using System.Buffers;
using System.Buffers.Binary;

namespace ZmqSharp.Zmtp;

/// <summary>
/// ZMTP frame encoder (the inverse of the parser): writes flags + size + body
/// per frame; a message is written atomically (never interleaved). Each frame
/// is produced as one <see cref="ReadOnlySequence{T}"/> (header + all segments)
/// and handed to the write sink in a single logical write (0015 section 6.1):
/// the socket sink scatter-writes it with one system call, the stream sink
/// writes the segments sequentially (the previous behavior, byte-identical).
/// </summary>
public sealed class ZmtpFrameEncoder
{
    private readonly IZWriteSink sink;
    private readonly byte[] header = new byte[9];

    public ZmtpFrameEncoder(Stream stream)
        : this(new StreamWriteSink(stream))
    {
    }

    internal ZmtpFrameEncoder(IZWriteSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        this.sink = sink;
    }

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
            await sink.WriteAsync(TwoPart(header.AsMemory(0, 9), body), token);
        }
        else
        {
            header[0] = (byte)ZmtpFrameFlags.Command;
            header[1] = (byte)length;
            await sink.WriteAsync(TwoPart(header.AsMemory(0, 2), body), token);
        }
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
            await sink.WriteAsync(TwoPart(BuildHeader(segment.Memory.Length, more), segment.Memory), token);
            return;
        }

        if (frame.TryGetValue(out ZSegments segments))
        {
            var length = 0L;
            foreach (var seg in segments) length += seg.Memory.Length;

            // The common segmented case is a single block; the two-part
            // sequence is allocation-free, the node chain is not.
            if (segments.Count == 1)
            {
                await sink.WriteAsync(TwoPart(BuildHeader(length, more), segments[0].Memory), token);
                return;
            }

            await sink.WriteAsync(ZSequence.Build(HeaderAndSegments(BuildHeader(length, more), segments)), token);
        }
    }

    public async ValueTask WriteFrameAsync(ReadOnlySequence<byte> frame, bool more, CancellationToken token = default)
    {
        await sink.WriteAsync(PrependHeader(BuildHeader(frame.Length, more), frame), token);
    }

    private ReadOnlyMemory<byte> BuildHeader(long length, bool more)
    {
        if (length > 255)
        {
            header[0] = (byte)((more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None) | ZmtpFrameFlags.LongSize);
            BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1, 8), length);
            return header.AsMemory(0, 9);
        }

        header[0] = (byte)(more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None);
        header[1] = (byte)length;
        return header.AsMemory(0, 2);
    }

    /// <summary>
    /// Builds a two-segment sequence (header + body) without a copy; an empty
    /// body falls back to the single-segment form. There is no
    /// <c>ReadOnlySequence(Memory, Memory)</c> constructor, so the two-part
    /// form costs two transient chain nodes (~96 B of gen-0 garbage per
    /// frame - comparable to the scatter send's BCL Task, outside the
    /// measured allocation gates). A future zero-copy scatter path (SAEA
    /// buffer lists, 0021 section 7) removes them.
    /// </summary>
    private static ReadOnlySequence<byte> TwoPart(ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second)
    {
        if (second.IsEmpty) return new ReadOnlySequence<byte>(first);

        ZSequenceSegment head = new(first, 0);
        var tail = new ZSequenceSegment(second, first.Length);
        head.SetNext(tail);
        return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
    }

    /// <summary>
    /// Prepends the frame header to a caller-provided sequence. Single-segment
    /// input uses the allocation-free two-part form; multi-segment input needs
    /// a transient node chain (only the scattered send path produces it).
    /// </summary>
    private static ReadOnlySequence<byte> PrependHeader(ReadOnlyMemory<byte> header, ReadOnlySequence<byte> frame)
    {
        if (frame.IsSingleSegment) return TwoPart(header, frame.First);

        ZSequenceSegment head = new(header, 0);
        var tail = head;
        var running = header.Length;
        foreach (var memory in frame)
        {
            var node = new ZSequenceSegment(memory, running);
            tail.SetNext(node);
            tail = node;
            running += memory.Length;
        }

        return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
    }

    private static IEnumerable<ReadOnlyMemory<byte>> HeaderAndSegments(ReadOnlyMemory<byte> header, ZSegments segments)
    {
        yield return header;
        foreach (var segment in segments) yield return segment.Memory;
    }

    /// <summary>
    /// The generic-transport sink: writes the frame's segments sequentially
    /// (the pre-sink behavior of the stream encoder), inside one gate
    /// acquisition held by the connection.
    /// </summary>
    private sealed class StreamWriteSink(Stream stream) : IZWriteSink
    {
        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            return stream.WriteAsync(bytes, token);
        }

        public async ValueTask WriteAsync(ReadOnlySequence<byte> sequence, CancellationToken token = default)
        {
            foreach (var memory in sequence) await stream.WriteAsync(memory, token);
        }
    }
}
