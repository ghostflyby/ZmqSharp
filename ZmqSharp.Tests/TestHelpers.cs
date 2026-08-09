using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

/// <summary>In-memory stream; caps the chunk size to simulate partial TCP reads.</summary>
internal sealed class ChunkedMemoryStream(byte[] data, int maxChunkSize = 0) : Stream
{
    private int position;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => data.Length;

    public override long Position
    {
        get => position;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
        => ReadInternal(buffer.AsSpan(offset, count));

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(ReadInternal(buffer.Span));

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private int ReadInternal(Span<byte> buffer)
    {
        if (position >= data.Length)
        {
            return 0;
        }

        var available = data.Length - position;
        var chunk = maxChunkSize > 0 ? Math.Min(maxChunkSize, available) : available;
        var count = Math.Min(buffer.Length, chunk);
        data.AsSpan(position, count).CopyTo(buffer);
        position += count;
        return count;
    }
}

/// <summary>Pool that tracks outstanding rentals, used to assert buffers are returned after message Dispose.</summary>
internal sealed class CountingMemoryPool : MemoryPool<byte>
{
    private readonly MemoryPool<byte> inner = Shared;
    private int outstanding;

    public int Outstanding => Volatile.Read(ref outstanding);

    public override int MaxBufferSize => inner.MaxBufferSize;

    public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
    {
        var owner = inner.Rent(minimumBufferSize);
        Interlocked.Increment(ref outstanding);
        return new TrackingOwner(this, owner);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }
    }

    private sealed class TrackingOwner(CountingMemoryPool pool, IMemoryOwner<byte> inner) : IMemoryOwner<byte>
    {
        private int disposed;

        public Memory<byte> Memory => inner.Memory;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                Interlocked.Decrement(ref pool.outstanding);
                inner.Dispose();
            }
        }
    }
}

/// <summary>
/// Pool that probes the receive path: counts every Rent and can be armed to
/// throw from Rent, proving whether an over-limit frame was allocated.
/// </summary>
internal sealed class ProbingMemoryPool : MemoryPool<byte>
{
    private readonly MemoryPool<byte> inner = Shared;
    private int rented;

    /// <summary>Total Rent calls since construction or the last <see cref="Reset"/>.</summary>
    public int Rentals => Volatile.Read(ref rented);

    /// <summary>When set, Rent throws before allocating anything.</summary>
    public Exception? FailOnRent { get; set; }

    public override int MaxBufferSize => inner.MaxBufferSize;

    public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
    {
        Interlocked.Increment(ref rented);
        if (FailOnRent is { } failure)
        {
            throw failure;
        }

        return inner.Rent(minimumBufferSize);
    }

    public void Reset() => Interlocked.Exchange(ref rented, 0);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }
    }
}

/// <summary>Test factory for building ZMessage instances directly.</summary>
internal static class MessageFactory
{
    public static ZMessage SingleFrame(byte[] payload)
        => new(new ZSingleMessage(new ZFrame(new ZSegment(payload, payload))));

    public static ZMessage PooledSingleFrame(MemoryPool<byte> pool, byte[] payload)
    {
        var owner = pool.Rent(payload.Length);
        payload.CopyTo(owner.Memory);
        return new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, owner.Memory[..payload.Length]))));
    }

    public static ZMessage SegmentedFrame(params byte[][] segments)
    {
        if (segments.Length == 1)
        {
            return SingleFrame(segments[0]);
        }

        return new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegments(
                [.. segments.Select(segment => new ZSegment(segment, segment))]))));
    }

    public static ZMessage Multipart(params byte[][] frames)
        => new(new ZMultiMessage(
            [.. frames.Select(frame => new ZFrame(new ZSegment(frame, frame)))]));

    public static ZMessage PooledMultipart(MemoryPool<byte> pool, params byte[][] frames)
    {
        var refs = new List<ZFrame>(frames.Length);
        foreach (var frame in frames)
        {
            var owner = pool.Rent(frame.Length);
            frame.CopyTo(owner.Memory);
            refs.Add(new ZFrame(new ZSegment(owner, owner.Memory[..frame.Length])));
        }

        return new ZMessage(new ZMultiMessage([.. refs]));
    }
}

/// <summary>Captures streamed frames (copied, since frames are borrowed).</summary>
internal sealed class FrameRecorder(Func<ZFrame, CancellationToken, bool>? onFrame = null) : IZMessageSink
{
    public List<byte[]> Frames { get; } = [];

    public List<bool> MoreFlags { get; } = [];

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        frame.TryGetValue(out ZSegment segment);
        Frames.Add(segment.Memory.ToArray());
        MoreFlags.Add(frame.More);
        return ValueTask.FromResult(onFrame?.Invoke(frame, token) ?? true);
    }

    public void OnConnectionEnded()
    {
    }
}

internal static class ZmtpTestRunner
{
    public static ZmtpParser CreateParser(IZConnection connection, IZMessageSink sink)
    {
        connection.SetFrameHandler((frame, ct) => sink.OnFrameAsync(frame, ct));
        return new ZmtpParser(connection);
    }

    public static async Task RunParserAsync(IZConnection connection, IZMessageSink sink)
    {
        using var parser = CreateParser(connection, sink);
        if (await parser.EstablishAsync())
        {
            await parser.ParseAsync();
        }
    }

    public static async Task RunParserAsync(ZmtpParser parser)
    {
        if (await parser.EstablishAsync())
        {
            await parser.ParseAsync();
        }
    }
}

/// <summary>ZMTP wire encoding helpers (tests only).</summary>
internal static class ZmtpTestData
{
    public static byte[] Greeting()
    {
        var result = new byte[64];
        result[0] = 0xFF;
        result[9] = 0x7F;
        result[10] = 3;
        "NULL"u8.CopyTo(result.AsSpan(12, 4));
        return result;
    }

    public static byte[] Ready(string socketType = "PAIR") => Frame(ReadyBody(socketType), command: true);

    public static byte[] ReadyBody(string socketType = "PAIR")
        => ReadyBodyWithProperties(("Socket-Type", socketType));

    public static byte[] ReadyWithProperties(params (string Name, string Value)[] properties)
        => Frame(ReadyBodyWithProperties(properties), command: true);

    public static byte[] ReadyBodyWithProperties(params (string Name, string Value)[] properties)
    {
        var body = new List<byte> { 5 };
        body.AddRange("READY"u8);
        foreach (var (name, value) in properties)
        {
            var nameBytes = Encoding.ASCII.GetBytes(name);
            var valueBytes = Encoding.UTF8.GetBytes(value);
            body.Add((byte)nameBytes.Length);
            body.AddRange(nameBytes);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(lengthBytes, valueBytes.Length);
            body.AddRange(lengthBytes);
            body.AddRange(valueBytes);
        }

        return [.. body];
    }

    public static byte[] ReadyWithRawProperty(ReadOnlySpan<byte> name, int valueLength)
        => Frame(ReadyBodyWithRawProperty(name, valueLength), command: true);

    private static byte[] ReadyBodyWithRawProperty(ReadOnlySpan<byte> name, int valueLength)
    {
        var body = new byte[6 + 1 + name.Length + 4];
        body[0] = 5;
        "READY"u8.CopyTo(body.AsSpan(1));
        var offset = 6;
        body[offset] = (byte)name.Length;
        offset++;
        name.CopyTo(body.AsSpan(offset));
        offset += name.Length;
        BinaryPrimitives.WriteInt32BigEndian(body.AsSpan(offset), valueLength);
        return body;
    }

    public static byte[] Error(string reason)
    {
        var bytes = Encoding.ASCII.GetBytes(reason);
        var body = new byte[1 + 5 + 1 + bytes.Length];
        body[0] = 5;
        "ERROR"u8.CopyTo(body.AsSpan(1));
        body[6] = (byte)bytes.Length;
        bytes.CopyTo(body.AsSpan(7));
        return Frame(body, command: true);
    }

    public static byte[] Frame(byte[] body, bool more = false, bool command = false, byte flagsOverride = 0)
    {
        var isLong = body.Length > 255;
        var flags = (byte)(
            (more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None)
            | (isLong ? ZmtpFrameFlags.LongSize : ZmtpFrameFlags.None)
            | (command ? ZmtpFrameFlags.Command : ZmtpFrameFlags.None)
            | (ZmtpFrameFlags)flagsOverride);
        if (!isLong)
        {
            var result = new byte[2 + body.Length];
            result[0] = flags;
            result[1] = (byte)body.Length;
            body.CopyTo(result.AsSpan(2));
            return result;
        }

        var longResult = new byte[9 + body.Length];
        longResult[0] = flags;
        BinaryPrimitives.WriteInt64BigEndian(longResult.AsSpan(1, 8), body.Length);
        body.CopyTo(longResult.AsSpan(9));
        return longResult;
    }

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(part => part.Length)];
        var offset = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result.AsSpan(offset));
            offset += part.Length;
        }

        return result;
    }
}
