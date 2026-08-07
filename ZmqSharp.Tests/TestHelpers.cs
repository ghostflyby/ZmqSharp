using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
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
    private readonly MemoryPool<byte> inner = MemoryPool<byte>.Shared;
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

/// <summary>Test factory for building ZMessage instances directly.</summary>
internal static class MessageFactory
{
    public static ZMessage Multipart(params byte[][] frames)
    {
        var data = new ZMessageData();
        foreach (var frame in frames)
        {
            data.AddSegment(new ZSegment { Origin = ZBufferOrigin.Owned, Memory = frame });
            data.AddFrame(data.SegmentCount - 1, 0, frame.Length);
        }

        return new ZMessage(data);
    }

    public static ZMessage PooledSingleFrame(MemoryPool<byte> pool, byte[] payload)
    {
        var owner = pool.Rent(payload.Length);
        payload.CopyTo(owner.Memory);
        var data = new ZMessageData();
        data.AddSegment(new ZSegment
        {
            Origin = ZBufferOrigin.Pooled,
            Owner = owner,
            Memory = owner.Memory[..payload.Length],
        });
        data.AddFrame(0, 0, payload.Length);
        return new ZMessage(data);
    }

    public static ZMessage PooledMultipart(MemoryPool<byte> pool, params byte[][] frames)
    {
        var data = new ZMessageData();
        foreach (var frame in frames)
        {
            var owner = pool.Rent(frame.Length);
            frame.CopyTo(owner.Memory);
            data.AddSegment(new ZSegment
            {
                Origin = ZBufferOrigin.Pooled,
                Owner = owner,
                Memory = owner.Memory[..frame.Length],
            });
            data.AddFrame(data.SegmentCount - 1, 0, frame.Length);
        }

        return new ZMessage(data);
    }

    /// <summary>Single frame spanning multiple segments (segmented frame).</summary>
    public static ZMessage SegmentedFrame(params byte[][] segments)
    {
        var data = new ZMessageData();
        var total = 0;
        foreach (var segment in segments)
        {
            data.AddSegment(new ZSegment { Origin = ZBufferOrigin.Owned, Memory = segment });
            total += segment.Length;
        }

        data.AddFrame(0, 0, total);
        return new ZMessage(data);
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

    public static byte[] Ready() => Frame("READY\0"u8.ToArray(), command: true);

    public static byte[] Error(string reason)
        => Frame(Encoding.ASCII.GetBytes($"ERROR\0{reason}"), command: true);

    public static byte[] Frame(byte[] body, bool more = false, bool command = false, byte flagsOverride = 0)
    {
        var isLong = body.Length > 255;
        var flags = (byte)((more ? 0b0001 : 0) | (isLong ? 0b0010 : 0) | (command ? 0b0100 : 0) | flagsOverride);
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
