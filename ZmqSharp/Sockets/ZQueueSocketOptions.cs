using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Queue socket configuration.</summary>
public sealed class ZQueueSocketOptions
{
    /// <summary>Receive queue capacity (per-peer HWM).</summary>
    public int ReceiveCapacity { get; init; } = 16;

    /// <summary>When set, enables the optional outbound channel with this capacity.</summary>
    public int? SendCapacity { get; init; }

    /// <summary>
    /// Receive materialization policy; defaults to the numeric
    /// <see cref="ZReceiveOptions"/> configuration, which accepts every frame
    /// pooled, contiguous up to <c>ContiguousFrameLimit</c> and segmented
    /// above it. The policy only decides allocation; the rejection limits
    /// below are enforced outside it.
    /// </summary>
    public IZReceivePolicy ReceivePolicy { get; init; } = new ZReceiveOptions();

    /// <summary>
    /// Maximum accepted frame length; a longer frame rejects the connection
    /// (0008 D3/D6). Defaults to effectively unlimited.
    /// </summary>
    public long MaxFrameLength { get; init; } = long.MaxValue;

    /// <summary>
    /// Maximum accepted accumulated message length; a larger total rejects
    /// the connection (0008 D3/D6). Defaults to effectively unlimited.
    /// </summary>
    public long MaxMessageLength { get; init; } = long.MaxValue;

    /// <summary>
    /// Maximum accepted frames per message; more frames reject the connection
    /// (0008 D3/D6). Defaults to effectively unlimited.
    /// </summary>
    public int MaxFramesPerMessage { get; init; } = int.MaxValue;

    /// <summary>
    /// Memory pool used for receive materialization and the send copy path;
    /// defaults to the shared pool. <c>MemoryPool&lt;byte&gt;.Shared</c>'s Dispose is
    /// a no-op, which makes it a safe default; the library never disposes an
    /// injected pool regardless, ownership stays with the caller.
    /// </summary>
    public MemoryPool<byte> Pool { get; init; } = MemoryPool<byte>.Shared;
}
