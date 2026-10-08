using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>
/// REQ composition root (0010 section 4; 0019): strict single in-flight
/// request over a round-robin peer selection, replies accepted only from the
/// current peer. The request core selects and sends to a peer through injected
/// capabilities; reply intake is the consume arm of the composed inbound policy (the
/// <see cref="ZReqCore"/>), so a <see cref="ZSocketOptions.MessageSink"/>
/// consumer is never hijacked by the protocol. Sends go through
/// <see cref="RequestAsync"/>; the generic base send path is unavailable.
/// </summary>
public sealed class ZReqSocket : ZSocketBase
{
    private readonly ZReqCore core;

    public ZReqSocket(ZSocketOptions? options = null) : this(Create(options ?? new ZSocketOptions())) { }

    private ZReqSocket((SocketRuntime Runtime, ZReqCore Core) components) : base(components.Runtime)
    {
        core = components.Core;
    }

    private static (SocketRuntime, ZReqCore) Create(ZSocketOptions options)
    {
        var runtime = new SocketRuntime(options, new ZNoDispatch("REQ sends through RequestAsync"), ZSocketTypes.Req);
        var core = new ZReqCore(() => runtime.PeerSnapshot, runtime.SendRequestToAsync, runtime.RetirePeer);
        runtime.ConfigureInbound(core);
        runtime.PeerRemoved += (peer, _) => core.OnPeerEnded(peer);
        return (runtime, core);
    }

    /// <summary>
    /// Sends a request to the next peer (round-robin) and waits for its reply.
    /// The message is consumed by the request; the returned reply is owned by
    /// the caller and disposed exactly once. Throws when a request is already
    /// in flight (strict alternation) or no peer is connected.
    /// </summary>
    public Task<ZMessage> RequestAsync(ZMessage message, CancellationToken token = default)
    {
        return core.RequestAsync(message, token);
    }

    /// <summary>
    /// Sends a request that borrows the caller's buffer instead of copying
    /// (0026 3.6): zero pool rent, zero copy for <c>byte[]</c>-backed memory
    /// (a non-array backing may be copied inside the awaited write). The caller must not modify
    /// the buffer until the returned task completes, including cancellation or failure.
    /// Completion waits until the send has stopped accessing the buffer. A synchronous
    /// throw (no peer, a request in flight) ends the borrow immediately.
    /// </summary>
    public Task<ZMessage> RequestAsync(ReadOnlyMemory<byte> request, CancellationToken token = default)
    {
        var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed(request))));
        try
        {
            return core.RequestAsync(message, token);
        }
        catch
        {
            // Synchronous throw (no peer, in-flight): nothing owns the message.
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a request with non-contiguous content, copied, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(ReadOnlySequence<byte> request, CancellationToken token = default)
    {
        var message = ZMessage.Copy(request);
        try
        {
            return core.RequestAsync(message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a multipart request, copied frame by frame, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        var message = ZMessage.Copy(frames);
        try
        {
            return core.RequestAsync(message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }

    /// <summary>Sends a multipart request from a <c>byte[][]</c> collection, copied, and waits for its reply (0026).</summary>
    public Task<ZMessage> RequestAsync(IEnumerable<byte[]> frames, CancellationToken token = default)
    {
        var message = ZMessage.Copy(frames);
        try
        {
            return core.RequestAsync(message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }
    }
}
