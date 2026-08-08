using System.Buffers;
using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>
/// Contiguous case of a frame: one buffer plus its ownership token. The owner
/// is either the byte[] itself (owned) or an IMemoryOwner (pooled); a borrowed
/// segment carries a no-op owner and Dispose never touches anything.
/// </summary>
public readonly struct ZSegment : IReadOnlyList<ZSegment>, IDisposable
{
    private sealed class NoopMemoryOwner : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => Memory<byte>.Empty;

        public void Dispose()
        {
        }
    }

    /// <summary>Borrowed marker owner: never releases.</summary>
    internal static readonly IMemoryOwner<byte> NoopOwner = new NoopMemoryOwner();

    private readonly object? owner;
    private readonly Memory<byte> memory;

    internal ZSegment(object owner, Memory<byte> memory)
    {
        this.owner = owner;
        this.memory = memory;
    }

    /// <summary>The segment content.</summary>
    public ReadOnlyMemory<byte> Memory => memory;

    /// <summary>Writable view, used by the parser to fill the buffer during materialization.</summary>
    internal Memory<byte> Writable => memory;

    /// <summary>True when the segment is owned (backed by a caller byte[]).</summary>
    public bool IsOwned => owner is byte[];

    /// <summary>True when the segment is a borrowed view (no-op owner).</summary>
    internal bool IsBorrowed => ReferenceEquals(owner, NoopOwner);

    /// <summary>Returns the backing array when owned; false otherwise.</summary>
    public bool GetOwnedArray(out byte[] array)
    {
        if (owner is byte[] owned)
        {
            array = owned;
            return true;
        }

        array = [];
        return false;
    }

    public int Count => 1;

    public ZSegment this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(index, 0);
            return this;
        }
    }

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<ZSegment> IEnumerable<ZSegment>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        if (owner is IMemoryOwner<byte> memoryOwner)
        {
            memoryOwner.Dispose();
        }
    }

    public struct Enumerator : IEnumerator<ZSegment>
    {
        private readonly ZSegment segment;
        private int state;

        internal Enumerator(ZSegment segment)
        {
            this.segment = segment;
            state = 0;
        }

        public ZSegment Current => state != 1 ? throw new InvalidOperationException("enumeration has not started or has already finished") : segment;

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

        public void Reset() => state = 0;

        public void Dispose()
        {
        }
    }
}
