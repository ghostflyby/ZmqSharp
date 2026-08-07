using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Owned message: holds ownership of the frame table and its segments; the
/// consumer is responsible for Dispose. Dispose is idempotent and returns only
/// Pooled segments; Owned segments just drop their references. After Dispose,
/// every data accessor throws ObjectDisposedException.
/// </summary>
public sealed class ZMessage : IZMessage, IDisposable
{
    private readonly ZMessageData data;
    private int disposed;

    internal ZMessage(ZMessageData data) => this.data = data;

    /// <summary>Builds a single-frame Owned message from a caller array (zero copy, Dispose never touches a pool).</summary>
    public static ZMessage FromOwned(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var messageData = new ZMessageData();
        messageData.AddSegment(new ZSegment
        {
            Origin = ZBufferOrigin.Owned,
            Memory = data,
        });
        messageData.AddFrame(0, 0, data.Length);
        return new ZMessage(messageData);
    }

    public int FrameCount => Data.FrameCount;

    public ZBufferOrigin GetOrigin(int frame) => Data.GetOrigin(frame);

    public ReadOnlySequence<byte> GetFrame(int index)
    {
        var data = Data;
        if (index < 0 || index >= data.FrameCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        return data.GetFrame(index);
    }

    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory)
    {
        var data = Data;
        if (index < 0 || index >= data.FrameCount)
        {
            memory = default;
            return false;
        }

        return data.TryGetContiguousFrame(index, out memory);
    }

    public ReadOnlySequence<byte> Whole => Data.Whole;

    /// <summary>Copies all frames into a new GC-managed array that the caller may keep permanently.</summary>
    public byte[] ToOwnedArray()
    {
        var data = Data;
        long total = 0;
        for (var i = 0; i < data.FrameCount; i++)
        {
            total += data.GetFrame(i).Length;
        }

        var result = GC.AllocateUninitializedArray<byte>(checked((int)total));
        var offset = 0;
        for (var i = 0; i < data.FrameCount; i++)
        {
            var frame = data.GetFrame(i);
            foreach (var memory in frame)
            {
                memory.Span.CopyTo(result.AsSpan(offset));
                offset += memory.Length;
            }
        }

        return result;
    }

    /// <summary>
    /// Only for single-frame, single-segment Pooled messages: transfers the return
    /// responsibility to the caller (deferred return, not permanent ownership).
    /// After a transfer, this message no longer returns that segment.
    /// </summary>
    public bool TryTakeOwner(out IMemoryOwner<byte>? owner)
    {
        var data = Data;
        if (data.FrameCount != 1 || !data.TryGetContiguousFrame(0, out _))
        {
            owner = null;
            return false;
        }

        var segment = data.GetSegment(0);
        if (segment.Origin != ZBufferOrigin.Pooled || segment.Owner is null)
        {
            owner = null;
            return false;
        }

        owner = segment.Owner;
        segment.Owner = null;
        return true;
    }

    /// <summary>
    /// Returns Pooled segments to their pool. Managed memory is released by the GC
    /// when the message becomes unreachable; no reference is nulled here.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            data.Dispose();
        }
    }

    private ZMessageData Data
    {
        get
        {
            if (Volatile.Read(ref disposed) == 1)
            {
                throw new ObjectDisposedException(nameof(ZMessage));
            }

            return data;
        }
    }
}
