using System.Buffers;
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
