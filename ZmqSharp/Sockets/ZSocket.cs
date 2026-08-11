using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>
/// Socket factory: the queue surface is the primary path with short names;
/// the callback surface is created through the *Callback entry points.
/// </summary>
public static class ZSocket
{
    public static ZQueueSocket<ZPairSocket> CreatePair(ZQueueSocketOptions? options = null)
        => new(new ZPairSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);

    public static ZQueueSocket<ZDealerSocket> CreateDealer(ZQueueSocketOptions? options = null)
        => new(new ZDealerSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);

    public static ZPairSocket CreatePairCallback(ZSocketOptions? options = null)
        => new(options ?? new ZSocketOptions());

    public static ZDealerSocket CreateDealerCallback(ZSocketOptions? options = null)
        => new(options ?? new ZSocketOptions());

    public static ZReqSocket CreateReq(ZSocketOptions? options = null)
        => new(options ?? new ZSocketOptions());

    public static ZRepSocket CreateRep(ZSocketOptions? options = null)
        => new(options ?? new ZSocketOptions());
}
