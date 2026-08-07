using System.Buffers;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>Socket configuration.</summary>
public sealed class ZSocketOptions
{
    /// <summary>When set, enables the receive channel with this capacity (HWM).</summary>
    public int? ReceiveChannelCapacity { get; init; }

    /// <summary>When set, enables the optional send channel with this capacity.</summary>
    public int? SendChannelCapacity { get; init; }

    /// <summary>Parser receive options; the policy is forced to Borrowed internally.</summary>
    public ZReceiveOptions Receive { get; init; } = new();

    public MemoryPool<byte>? Pool { get; init; }
}
