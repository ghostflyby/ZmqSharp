using System.Buffers;
using System.Threading.Channels;

namespace ZmqSharp.Sockets;

/// <summary>Queue socket configuration.</summary>
public sealed class ZQueueSocketOptions
{
    /// <summary>
    /// Per-peer receive queue factory (0009); the library forces
    /// SingleReader and the factory's SingleWriter per connection. Defaults
    /// to a bounded SPSC queue with capacity 16. BCL channel options convert
    /// implicitly into a factory, so <c>new BoundedChannelOptions(16)</c> is
    /// assignable here.
    /// </summary>
    public ZQueueFactory ReceiveQueueFactory { get; init; } = new BoundedChannelOptions(16) { SingleWriter = true };

    /// <summary>When set, enables the optional outbound channel built by this factory (0009).</summary>
    public ZQueueFactory? SendQueueFactory { get; init; }

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
