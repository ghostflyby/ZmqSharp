using System.Buffers;
using System.Collections;

namespace ZmqSharp;

/// <summary>Single-frame message: exactly one ZFrame.</summary>
public readonly struct ZSingleMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZFrame frame;

    internal ZSingleMessage(ZFrame frame)
    {
        this.frame = frame;
    }

    /// <summary>Zero-copy single frame: the caller transfers ownership of the array.</summary>
    public static ZSingleMessage FromOwned(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        return new ZSingleMessage(new ZFrame(new ZSegment(data, 0, data.Length)));
    }

    /// <summary>
    /// Zero-copy single frame from a pooled buffer: the caller transfers the
    /// <see cref="IMemoryOwner{T}"/>; it is disposed with the message (0026).
    /// </summary>
    public static ZSingleMessage FromPooled(IMemoryOwner<byte> owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return new ZSingleMessage(new ZFrame(new ZSegment(owner, 0, owner.Memory.Length)));
    }

    /// <summary>Single frame copied into an owned buffer (0026); the caller's memory stays usable.</summary>
    public static ZSingleMessage Copy(ReadOnlyMemory<byte> data)
    {
        var owner = MemoryPool<byte>.Shared.Rent(data.Length);
        data.CopyTo(owner.Memory);
        return new ZSingleMessage(new ZFrame(new ZSegment(owner, 0, data.Length)));
    }

    /// <summary>
    /// Single frame with non-contiguous content, copied segment by segment
    /// (0026). A single-segment sequence collapses to the contiguous form.
    /// </summary>
    public static ZSingleMessage Copy(ReadOnlySequence<byte> frame)
    {
        if (frame.IsSingleSegment)
        {
            var single = frame.First;
            var owner = MemoryPool<byte>.Shared.Rent(single.Length);
            single.CopyTo(owner.Memory);
            return new ZSingleMessage(new ZFrame(new ZSegment(owner, 0, single.Length)));
        }

        var segments = new List<ZSegment>();
        foreach (var memory in frame)
        {
            var owner = MemoryPool<byte>.Shared.Rent(memory.Length);
            memory.CopyTo(owner.Memory);
            segments.Add(new ZSegment(owner, 0, memory.Length));
        }

        return new ZSingleMessage(new ZFrame(new ZSegments([.. segments])));
    }

    public int Count => 1;

    public ZFrame this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(index, 0);
            return frame;
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(frame);
    }

    IEnumerator<ZFrame> IEnumerable<ZFrame>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
        frame.Dispose();
    }

    public struct Enumerator : IEnumerator<ZFrame>
    {
        private readonly ZFrame frame;
        private int state;

        internal Enumerator(ZFrame frame)
        {
            this.frame = frame;
            state = 0;
        }

        public ZFrame Current => state != 1
            ? throw new InvalidOperationException("enumeration has not started or has already finished")
            : frame;

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (state == 0)
            {
                state = 1;
                return true;
            }

            state = 2;
            return false;
        }

        public void Reset()
        {
            state = 0;
        }

        public void Dispose()
        {
        }
    }
}
