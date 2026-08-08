using System.Buffers;
using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>
/// Owned multipart message: each frame is one segment (contiguous) or spans
/// several segments (non-contiguous); frames are the array elements. Dispose
/// is idempotent and returns Pooled segments.
/// </summary>
public sealed class ZMultiMessage : IZMessage
{
    private readonly ZFrameSegments[] frames;
    private int disposed;

    internal ZMultiMessage(ZFrameSegments[] frames) => this.frames = frames;

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

            return FrameSequence(frames[index]);
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
        private readonly ZFrameSegments[] frames;
        private int index;

        internal Enumerator(ZFrameSegments[] frames)
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

                return FrameSequence(frames[index]);
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
        if (index < 0 || index >= frames.Length || frames[index].Single is not { } single)
        {
            memory = default;
            return false;
        }

        memory = single.Memory;
        return true;
    }

    public bool TryGetOwnedArray(int index, out byte[] array)
    {
        ThrowIfDisposed();
        if (index < 0 || index >= frames.Length ||
            frames[index].Single is not { Owner: byte[] owned })
        {
            array = [];
            return false;
        }

        array = owned;
        return true;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        foreach (var frame in frames)
        {
            if (frame.Single is { } single)
            {
                single.Release();
            }
            else if (frame.Many is { } many)
            {
                foreach (var segment in many)
                {
                    segment.Release();
                }
            }
        }
    }

    private static ReadOnlySequence<byte> FrameSequence(ZFrameSegments frame)
    {
        if (frame.Single is { } single)
        {
            return new ReadOnlySequence<byte>(single.Memory);
        }

        if (frame.Many is { } many)
        {
            return ZSequence.Build(IterateSegments(many));
        }

        return ReadOnlySequence<byte>.Empty;
    }

    private static IEnumerable<ReadOnlyMemory<byte>> IterateSegments(ZBufferRef[] segments)
    {
        foreach (var segment in segments)
        {
            yield return segment.Memory;
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref disposed) != 1)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(ZMultiMessage));
    }
}
