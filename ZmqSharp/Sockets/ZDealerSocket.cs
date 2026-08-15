using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>DEALER composition root: round-robin outbound, fair-queue inbound, queue receive surface (0007 section 5, 0023).</summary>
public sealed class ZDealerSocket(ZSocketOptions? options = null)
    : ZQueueSocketBase(options ?? new ZSocketOptions(), new ZRoundRobinDispatch(), ZSocketTypes.Dealer)
{
    /// <summary>Direct send, round-robin across peers (0024); the message is disposed after the send.</summary>
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
