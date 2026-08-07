using System.Buffers;
using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>
/// Owned single-frame message. The frame is one buffer (contiguous) or spans
/// several segments (non-contiguous). Dispose is idempotent and returns Pooled
/// segments; after Dispose every accessor throws ObjectDisposedException.
/// </summary>
public sealed class ZMessage : IZMessage
{
    private readonly ZBufferRef first;
    private readonly ZBufferRef[]? more;
    private int disposed;

    internal ZMessage(ZBufferRef first, ZBufferRef[]? more = null)
    {
        this.first = first;
        this.more = more;
    }

    /// <summary>Builds a message from a caller array (zero copy, Dispose never touches a pool).</summary>
    public static ZMessage FromOwned(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        return new ZMessage(new ZBufferRef(data, data));
    }

    public int Count
    {
        get
        {
            ThrowIfDisposed();
            return 1;
        }
    }

    public ReadOnlySequence<byte> this[int index]
    {
        get
        {
            ThrowIfDisposed();
            ArgumentOutOfRangeException.ThrowIfNotEqual(index, 0);

            return more is null
                ? new ReadOnlySequence<byte>(first.Memory)
                : BuildSequence();
        }
    }

    public Enumerator GetEnumerator()
    {
        ThrowIfDisposed();
        return new Enumerator(this);
    }

    IEnumerator<ReadOnlySequence<byte>> IEnumerable<ReadOnlySequence<byte>>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Struct enumerator: zero allocation for foreach.</summary>
    public struct Enumerator : IEnumerator<ReadOnlySequence<byte>>
    {
        private readonly ZMessage message;
        private int state;

        internal Enumerator(ZMessage message)
        {
            this.message = message;
            state = 0;
        }

        public ReadOnlySequence<byte> Current
        {
            get
            {
                if (state != 1)
                {
                    throw new InvalidOperationException("enumeration has not started or has already finished");
                }

                return message[0];
            }
        }

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

    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory)
    {
        ThrowIfDisposed();
        if (index != 0 || more is not null)
        {
            memory = default;
            return false;
        }

        memory = first.Memory;
        return true;
    }

    internal int SegmentCount
    {
        get
        {
            ThrowIfDisposed();
            return 1 + (more?.Length ?? 0);
        }
    }

    internal ZBufferRef GetSegment(int index)
    {
        ThrowIfDisposed();
        if (index == 0)
        {
            return first;
        }

        if (more is not null && index <= more.Length)
        {
            return more[index - 1];
        }

        throw new ArgumentOutOfRangeException(nameof(index));
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        first.Release();
        if (more is null) return;
        foreach (var segment in more)
        {
            segment.Release();
        }
    }

    private ReadOnlySequence<byte> BuildSequence()
    {
        var segments = new List<ReadOnlyMemory<byte>>(1 + (more?.Length ?? 0)) { first.Memory };
        if (more is null) return ZSequence.Build([.. segments]);
        foreach (var segment in more)
        {
            segments.Add(segment.Memory);
        }

        return ZSequence.Build([.. segments]);
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref disposed) == 1)
        {
            throw new ObjectDisposedException(nameof(ZMessage));
        }
    }
}
