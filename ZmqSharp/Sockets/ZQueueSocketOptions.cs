using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Queue socket configuration.</summary>
public sealed class ZQueueSocketOptions
{
    /// <summary>Receive queue capacity (per-peer HWM).</summary>
    public int ReceiveCapacity { get; init; } = 16;

    /// <summary>When set, enables the optional outbound channel with this capacity.</summary>
    public int? SendCapacity { get; init; }

    /// <summary>Receive materialization policy; null = pooled.</summary>
    public ZReceiveOptions? ReceivePolicy { get; init; }

    public MemoryPool<byte>? Pool { get; init; }
}
