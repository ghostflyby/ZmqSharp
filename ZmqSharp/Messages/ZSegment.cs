using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>Internal segment: a chunk of filled data and its origin.</summary>
internal sealed class ZSegment
{
    public ZBufferOrigin Origin { get; init; } = ZBufferOrigin.Pooled;

    /// <summary>Held when Pooled; set to null after TryTakeOwner transfers ownership.</summary>
    public IMemoryOwner<byte>? Owner { get; set; }

    public Memory<byte> Memory { get; set; }

    public void Dispose()
    {
        if (Owner is null) return;
        Owner.Dispose();
        Owner = null;
    }
}
