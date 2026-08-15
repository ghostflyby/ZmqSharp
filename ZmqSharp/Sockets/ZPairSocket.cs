using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>PAIR composition root: single peer, no routing, queue receive surface (0007 section 5, 0023).</summary>
public sealed class ZPairSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZSinglePeerDispatch(), ZSocketTypes.Pair)
{
    /// <summary>Direct send to the single peer (0024); the message is disposed after the send.</summary>
    public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        return SendAsyncCore(message, token);
    }

    /// <summary>Direct send: copies the payload into an owned message before routing.</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        return SendAsyncCore(bytes, token);
    }
}
