using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Socket configuration.</summary>
public sealed class ZSocketOptions
{
    public MemoryPool<byte>? Pool { get; init; }
}
