using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Internal storage unit: one contiguous buffer plus its ownership token.
/// Owner is either the byte[] itself (owned) or an IMemoryOwner (pooled).
/// </summary>
internal readonly struct ZBufferRef(object owner, ReadOnlyMemory<byte> memory)
{
    public object Owner { get; } = owner;

    public ReadOnlyMemory<byte> Memory { get; } = memory;

    public void Release()
    {
        if (Owner is IMemoryOwner<byte> memoryOwner)
        {
            memoryOwner.Dispose();
        }
    }
}
