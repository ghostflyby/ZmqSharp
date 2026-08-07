namespace ZmqSharp.Sockets;

/// <summary>Socket factory: types differ only in the scheduling policy.</summary>
public static class ZSocket
{
    public static IZSocket Create(ZSocketType type, ZSocketOptions? options = null)
    {
        options ??= new ZSocketOptions();
        return type switch
        {
            ZSocketType.Pair => new ZSocketBase(new PairPolicy(), options),
            ZSocketType.Dealer => new ZSocketBase(new FairPolicy(), options),
            _ => throw new ArgumentOutOfRangeException(nameof(type)),
        };
    }
}
