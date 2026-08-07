using System.Buffers;
using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>
/// Owned multipart message: each frame is one buffer (contiguous per frame);
/// frames are the array elements. Dispose is idempotent and returns Pooled
/// segments.
/// </summary>
public sealed class ZMultiMessage : IZMessage
{
    private readonly ZBufferRef[] frames;
    private int disposed;

    internal ZMultiMessage(ZBufferRef[] frames) => this.frames = frames;

    public int Count
    {
        get
        {
            ThrowIfDisposed();
            return frames.Length;
        }
    }

    public ReadOnlySequence<byte> this[int index]
    {
        get
        {
            ThrowIfDisposed();
            if (index < 0 || index >= frames.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return new ReadOnlySequence<byte>(frames[index].Memory);
        }
    }

    public Enumerator GetEnumerator()
    {
        ThrowIfDisposed();
        return new Enumerator(frames);
    }

    IEnumerator<ReadOnlySequence<byte>> IEnumerable<ReadOnlySequence<byte>>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Struct enumerator: zero allocation for foreach.</summary>
    public struct Enumerator : IEnumerator<ReadOnlySequence<byte>>
    {
        private readonly ZBufferRef[] frames;
        private int index;

        internal Enumerator(ZBufferRef[] frames)
        {
            this.frames = frames;
            index = -1;
        }

        public ReadOnlySequence<byte> Current
        {
            get
            {
                if (index < 0 || index >= frames.Length)
                {
                    throw new InvalidOperationException("enumeration has not started or has already finished");
                }

                return new ReadOnlySequence<byte>(frames[index].Memory);
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (index + 1 < frames.Length)
            {
                index++;
                return true;
            }

            index = frames.Length;
            return false;
        }

        public void Reset() => index = -1;

        public void Dispose()
        {
        }
    }

    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory)
    {
        ThrowIfDisposed();
        if (index < 0 || index >= frames.Length)
        {
            memory = default;
            return false;
        }

        memory = frames[index].Memory;
        return true;
    }

    public bool TryGetOwnedArray(int index, out byte[] array)
    {
        ThrowIfDisposed();
        if (index < 0 || index >= frames.Length || frames[index].Owner is not byte[] owned)
        {
            array = [];
            return false;
        }

        array = owned;
        return true;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        foreach (var frame in frames)
        {
            frame.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref disposed) != 1) return;
        throw new ObjectDisposedException(nameof(ZMultiMessage));
    }
}
