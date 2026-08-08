using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Internal storage unit: one contiguous buffer plus its ownership token.
/// Owner is either the byte[] itself (owned) or an IMemoryOwner (pooled).
/// </summary>
internal readonly struct ZBufferRef(object owner, Memory<byte> memory)
{
    private sealed class NoopMemoryOwner : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => Memory<byte>.Empty;

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Borrowed marker owner: never releases; keeps the "owner is byte[] or
    /// IMemoryOwner" invariant for consumers.
    /// </summary>
    internal static readonly IMemoryOwner<byte> NoopOwner = new NoopMemoryOwner();

    public object Owner { get; } = owner;

    public ReadOnlyMemory<byte> Memory => memory;

    /// <summary>Writable view, used by the parser to fill the buffer during materialization.</summary>
    internal Memory<byte> Writable => memory;

    public void Release()
    {
        if (Owner is IMemoryOwner<byte> memoryOwner)
        {
            memoryOwner.Dispose();
        }
    }
}
