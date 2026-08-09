using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Socket configuration.</summary>
public sealed class ZSocketOptions
{
    /// <summary>
    /// Memory pool used for the send copy path; defaults to the shared pool.
    /// <c>MemoryPool&lt;byte&gt;.Shared</c>'s Dispose is a no-op, which makes it a
    /// safe default; the library never disposes an injected pool regardless,
    /// ownership stays with the caller.
    /// </summary>
    public MemoryPool<byte> Pool { get; init; } = MemoryPool<byte>.Shared;
}
