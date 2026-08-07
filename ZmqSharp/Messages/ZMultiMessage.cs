using System.Buffers;

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

    public int FrameCount
    {
        get
        {
            ThrowIfDisposed();
            return frames.Length;
        }
    }

    public ReadOnlySequence<byte> GetFrame(int index)
    {
        ThrowIfDisposed();
        if (index < 0 || index >= frames.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        return new ReadOnlySequence<byte>(frames[index].Memory);
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

    public ReadOnlySequence<byte> Payload
    {
        get
        {
            ThrowIfDisposed();
            if (frames.Length == 1)
            {
                return new ReadOnlySequence<byte>(frames[0].Memory);
            }

            return ZSequence.Build([.. frames.Select(frame => frame.Memory)]);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            foreach (var frame in frames)
            {
                frame.Release();
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref disposed) == 1)
        {
            throw new ObjectDisposedException(nameof(ZMultiMessage));
        }
    }
}
