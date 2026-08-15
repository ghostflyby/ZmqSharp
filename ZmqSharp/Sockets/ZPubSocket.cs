using ZmqSharp.Patterns;

namespace ZmqSharp;

/// <summary>
/// PUB composition root (0013): send-only broadcast. The message's first
/// frame is the topic; every connected peer receives the full message via the
/// base selective send path, driven by <see cref="ZBroadcastDispatch"/>.
/// Non-sealed with an internal type/inbound-taking constructor so XPUB can
/// reuse the broadcast send.
/// </summary>
public class ZPubSocket : ZQueueSocketBase
{
    public ZPubSocket(ZSocketOptions? options = null)
        : this(options ?? new ZSocketOptions(), ZSocketTypes.Pub)
    {
    }

    internal ZPubSocket(ZSocketOptions options, ZSocketType type, IZInboundPolicy? inbound = null)
        : base(options, new ZBroadcastDispatch(), type, inbound)
    {
    }

    /// <summary>
    /// Direct send, broadcast to every peer (0024; ZXPubSocket inherits this
    /// surface). The message is disposed after the loop.
    /// </summary>
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
