using System.Buffers;
using System.Collections;

namespace ZmqSharp;

/// <summary>
/// Contiguous case of a frame: one buffer plus its ownership token. The owner
/// is the byte[] itself (owned), an IMemoryOwner (pooled), or a borrowed view
/// (a caller <see cref="ReadOnlyMemory{T}"/> of any backing). Borrowed
/// segments never take ownership - Dispose is a no-op for them (0026 3.6).
/// Content is stored as an offset and length into the owner;
/// <see cref="Memory"/> reacquires and slices on every access, so the segment
/// stores no <c>Memory&lt;byte&gt;</c> field (0006 3.4).
/// </summary>
public readonly struct ZSegment : IReadOnlyList<ZSegment>, IDisposable
{
    private readonly object? owner;
    private readonly int offset;
    private readonly int length;
    private readonly bool isBorrowed;

    internal ZSegment(object owner, int offset, int length)
        : this(owner, offset, length, false)
    {
    }

    private ZSegment(object owner, int offset, int length, bool isBorrowed)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        this.owner = owner;
        this.offset = offset;
        this.length = length;
        this.isBorrowed = isBorrowed;
    }

    /// <summary>
    /// Borrowed segment: refers to the parser's scratch owner without taking
    /// ownership, so <see cref="Dispose"/> is a no-op. Valid only while the
    /// scratch source outlives the frame (0006 3.4).
    /// </summary>
    internal static ZSegment Borrowed(IMemoryOwner<byte> source, int offset, int length)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new ZSegment(source, offset, length, true);
    }

    /// <summary>
    /// Borrowed view: refers to a caller <see cref="ReadOnlyMemory{T}"/> of
    /// any backing (<c>byte[]</c>, <c>string</c>, a custom
    /// <see cref="MemoryManager{T}"/>) without taking ownership, so
    /// <see cref="Dispose"/> is a no-op. The caller must not modify the memory
    /// until the send completes (0026 3.6).
    /// </summary>
    internal static ZSegment Borrowed(ReadOnlyMemory<byte> memory)
    {
        return new ZSegment(new BorrowedMemory(memory), 0, memory.Length, true);
    }

    /// <summary>The segment content; reacquires the owner memory on every access.</summary>
    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            if (owner is BorrowedMemory borrowed) return borrowed.Memory.Slice(offset, length);

            return Reacquire().Slice(offset, length);
        }
    }

    /// <summary>Writable view, used by the parser to fill the buffer during materialization.</summary>
    internal Memory<byte> Writable
    {
        get
        {
            if (owner is BorrowedMemory)
                throw new InvalidOperationException("a borrowed view segment has no writable view");

            return Reacquire().Slice(offset, length);
        }
    }

    /// <summary>True when the segment is owned (backed by a caller byte[]).</summary>
    public bool IsOwned => owner is byte[];

    /// <summary>True when the segment is a borrowed view (parser scratch source).</summary>
    internal bool IsBorrowed => isBorrowed;

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

    private Memory<byte> Reacquire()
    {
        return owner switch
        {
            byte[] array => array,
            IMemoryOwner<byte> pooled => pooled.Memory,
            _ => Memory<byte>.Empty
        };
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

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    IEnumerator<ZSegment> IEnumerable<ZSegment>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
        // A pooled owner is released; a borrowed segment must never release
        // the parser's scratch. Owner-specific behavior after disposal is
        // neither caught nor normalized (0005 section 2.1).
        if (!isBorrowed && owner is IMemoryOwner<byte> memoryOwner) memoryOwner.Dispose();
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

        public ZSegment Current => state != 1
            ? throw new InvalidOperationException("enumeration has not started or has already finished")
            : segment;

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

/// <summary>
/// The owner of a borrowed-view segment (0026 3.6): holds the caller's
/// <see cref="ReadOnlyMemory{T}"/> of any backing without taking ownership.
/// One small allocation per borrowed send; the receive hot path never builds
/// one of these.
/// </summary>
internal sealed class BorrowedMemory(ReadOnlyMemory<byte> memory)
{
    public ReadOnlyMemory<byte> Memory { get; } = memory;
}
