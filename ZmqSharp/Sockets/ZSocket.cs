namespace ZmqSharp.Sockets;

/// <summary>Socket factory: each socket type is its own subtype (libzmq-style).</summary>
public static class ZSocket
{
    public static IZSocket Create(ZSocketType type, ZSocketOptions? options = null)
    {
        options ??= new ZSocketOptions();
        return type switch
        {
            ZSocketType.Pair => new ZPairSocket(options),
            ZSocketType.Dealer => new ZDealerSocket(options),
            _ => throw new ArgumentOutOfRangeException(nameof(type)),
        };
    }

    /// <summary>Creates a queue socket: wraps the low-level socket and takes over its receive callback.</summary>
    public static ZQueueSocket CreateQueue(ZSocketType type, ZQueueSocketOptions? options = null)
    {
        options ??= new ZQueueSocketOptions();
        var socket = Create(type, new ZSocketOptions { Pool = options.Pool });
        return new ZQueueSocket(socket, options);
    }
}
