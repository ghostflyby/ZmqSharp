using System.Buffers;
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

    /// <summary>
    /// Direct send that borrows the caller's buffer instead of copying
    /// (0026 3.6): zero-copy, no pool rent, for any <see cref="ReadOnlyMemory{T}"/>
    /// backing. The caller must not modify the buffer until the returned task
    /// completes; after the await the buffer is free again.
    /// </summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed(bytes))));
        return SendAsyncCore(message, token);
    }

    /// <summary>Direct send of a single frame with non-contiguous content, copied (0026).</summary>
    public ValueTask SendAsync(ReadOnlySequence<byte> frame, CancellationToken token = default)
    {
        return SendAsyncCore(ZMessage.Copy(frame), token);
    }

    /// <summary>Direct send of a multipart message, copied frame by frame (0026).</summary>
    public ValueTask SendAsync(IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        return SendAsyncCore(ZMessage.Copy(frames), token);
    }

    /// <summary>Direct send of a multipart message from a <c>byte[][]</c> collection, copied (0026).</summary>
    public ValueTask SendAsync(IEnumerable<byte[]> frames, CancellationToken token = default)
    {
        return SendAsyncCore(ZMessage.Copy(frames), token);
    }
}
