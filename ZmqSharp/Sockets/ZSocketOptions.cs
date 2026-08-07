using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Socket configuration.</summary>
public sealed class ZSocketOptions
{
    /// <summary>When set, enables the receive channel with this capacity (HWM).</summary>
    public int? ReceiveChannelCapacity { get; init; }

    /// <summary>When set, enables the optional send channel with this capacity.</summary>
    public int? SendChannelCapacity { get; init; }

    public MemoryPool<byte>? Pool { get; init; }
}
