using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>ZMTP framing over a single writer. The session serializes whole messages.</summary>
public sealed class ZmtpFrameEncoder
{
    private readonly IZByteWriter _writer;
    private readonly byte[] _header = new byte[9];
    private readonly FrameSequence _wire = new();
    private readonly FrameSequence _logical = new();

    public ZmtpFrameEncoder(Stream stream) : this(new StreamWriter(stream)) { }

    public ZmtpFrameEncoder(IZByteWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        _writer = writer;
    }

    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => WriteFrameAsync(new ZmtpFrameData { Flags = ZmtpFrameFlags.Command, Body = new ReadOnlySequence<byte>(body) }, token);

    public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
    {
        for (var i = 0; i < message.Count; i++)
            await WriteFrameAsync(new ZmtpFrameData
            {
                Flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None,
                Body = _logical.FromFrame(message[i])
            }, token);
    }

    public ValueTask WriteFrameAsync(ReadOnlySequence<byte> body, bool more, CancellationToken token = default)
        => WriteFrameAsync(new ZmtpFrameData { Body = body, Flags = more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None }, token);

    public ValueTask WriteFrameAsync(ZmtpFrameData frame, CancellationToken token = default)
    {
        ValidateFlags(frame.Flags);
        var length = frame.Body.Length;
        var longSize = length > 255;
        _header[0] = (byte)(frame.Flags | (longSize ? ZmtpFrameFlags.LongSize : ZmtpFrameFlags.None));
        if (longSize) BinaryPrimitives.WriteInt64BigEndian(_header.AsSpan(1), length);
        else _header[1] = (byte)length;
        _wire.Clear();
        _wire.Add(_header.AsMemory(0, longSize ? 9 : 2));
        foreach (var segment in frame.Body) _wire.Add(segment);
        return _writer.WriteAsync(_wire.Sequence, token);
    }

    internal static void ValidateFlags(ZmtpFrameFlags flags)
    {
        if ((flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            (flags & (ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid logical frame flags");
    }

    private sealed class StreamWriter(Stream stream) : IZByteWriter
    {
        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
            => stream.WriteAsync(bytes, token);

        public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
        {
            foreach (var segment in bytes) await stream.WriteAsync(segment, token);
        }
    }
}
