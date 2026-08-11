using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>
/// Socket factory: the queue surface is the primary path with short names;
/// the callback surface is created through the *Callback entry points.
/// </summary>
public static class ZSocket
{
    public static ZQueueSocket<ZPairSocket> CreatePair(ZQueueSocketOptions? options = null)
    {
        return new ZQueueSocket<ZPairSocket>(
            new ZPairSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);
    }

    public static ZQueueSocket<ZDealerSocket> CreateDealer(ZQueueSocketOptions? options = null)
    {
        return new ZQueueSocket<ZDealerSocket>(
            new ZDealerSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);
    }

    public static ZPairSocket CreatePairCallback(ZSocketOptions? options = null)
    {
        return new ZPairSocket(options ?? new ZSocketOptions());
    }

    public static ZDealerSocket CreateDealerCallback(ZSocketOptions? options = null)
    {
        return new ZDealerSocket(options ?? new ZSocketOptions());
    }

    public static ZReqSocket CreateReq(ZSocketOptions? options = null)
    {
        return new ZReqSocket(options ?? new ZSocketOptions());
    }

    public static ZRepSocket CreateRep(ZSocketOptions? options = null)
    {
        return new ZRepSocket(options ?? new ZSocketOptions());
    }

    public static ZPushSocket CreatePush(ZSocketOptions? options = null)
    {
        return new ZPushSocket(options ?? new ZSocketOptions());
    }

    public static ZQueueSocket<ZPullSocket> CreatePull(ZQueueSocketOptions? options = null)
    {
        return new ZQueueSocket<ZPullSocket>(
            new ZPullSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);
    }

    public static ZQueueSocket<ZRouterSocket> CreateRouter(ZQueueSocketOptions? options = null)
    {
        return new ZQueueSocket<ZRouterSocket>(
            new ZRouterSocket(new ZSocketOptions { Pool = options?.Pool ?? MemoryPool<byte>.Shared }), options);
    }

    public static ZRouterSocket CreateRouterCallback(ZSocketOptions? options = null)
    {
        return new ZRouterSocket(options ?? new ZSocketOptions());
    }

    public static ZPubSocket CreatePub(ZSocketOptions? options = null)
    {
        return new ZPubSocket(options ?? new ZSocketOptions());
    }

    public static ZSubSocket CreateSubCallback(ZSocketOptions? options = null)
    {
        return new ZSubSocket(options ?? new ZSocketOptions());
    }

    public static ZXSubSocket CreateXSubCallback(ZSocketOptions? options = null)
    {
        return new ZXSubSocket(options ?? new ZSocketOptions());
    }

    public static ZXPubSocket CreateXPub(ZSocketOptions? options = null)
    {
        return new ZXPubSocket(options ?? new ZSocketOptions());
    }
}
