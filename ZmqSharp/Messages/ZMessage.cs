using System.Buffers;

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

    public int FrameCount
    {
        get
        {
            ThrowIfDisposed();
            return 1;
        }
    }

    public ReadOnlySequence<byte> GetFrame(int index)
    {
        ThrowIfDisposed();
        if (index != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        return more is null
            ? new ReadOnlySequence<byte>(first.Memory)
            : BuildSequence();
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

    public ReadOnlySequence<byte> Payload => GetFrame(0);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            first.Release();
            if (more is not null)
            {
                foreach (var segment in more)
                {
                    segment.Release();
                }
            }
        }
    }

    private ReadOnlySequence<byte> BuildSequence()
    {
        var segments = new List<ReadOnlyMemory<byte>>(1 + (more?.Length ?? 0)) { first.Memory };
        if (more is not null)
        {
            foreach (var segment in more)
            {
                segments.Add(segment.Memory);
            }
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
