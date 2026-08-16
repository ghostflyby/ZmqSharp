using System.Buffers;
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

    /// <summary>
    /// Direct send that borrows the caller's buffer instead of copying
    /// (0026 3.6): zero pool rent, zero copy for <c>byte[]</c>-backed memory
    /// (a non-array backing may be copied inside the awaited write). The
    /// caller must not modify the buffer until the returned task completes;
    /// after the await the buffer is free again. Use
    /// <c>ZMessage.Copy</c> + <see cref="SendAsync(ZMessage, CancellationToken)"/>
    /// when the buffer must stay mutable across the send.
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
