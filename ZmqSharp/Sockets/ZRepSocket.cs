using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>
/// REP composition root (0010 section 4; 0019): fair intake of requests
/// serialized across peers (one at a time, strict alternation), delivered as
/// a <see cref="ZRequestContext"/>; replies route back to the originating
/// peer. The request intake is the consume arm of the composed inbound policy
/// (the <see cref="ZRepCore"/>), so a <see cref="ZSocketOptions.MessageSink"/>
/// consumer is never hijacked by the protocol.
/// </summary>
public sealed class ZRepSocket : ZSocketBase
{
    private readonly ZRepCore core;
    private readonly RequestHandlerSlot handlers;

    public ZRepSocket(ZSocketOptions? options = null) : this(Create(options ?? new ZSocketOptions())) { }

    private ZRepSocket((SocketRuntime Runtime, ZRepCore Core, RequestHandlerSlot Handlers) components)
        : base(components.Runtime)
    {
        core = components.Core;
        handlers = components.Handlers;
    }

    private static (SocketRuntime, ZRepCore, RequestHandlerSlot) Create(ZSocketOptions options)
    {
        var runtime = new SocketRuntime(options, new ZNoDispatch("REP replies through SendReplyAsync"), ZSocketTypes.Rep);
        var handlers = new RequestHandlerSlot();
        var core = new ZRepCore(handlers.InvokeAsync, runtime.SendToAsync);
        runtime.ConfigureInbound(core);
        return (runtime, core, handlers);
    }

    /// <summary>Handles requests serially; the context is valid until the handler completes.</summary>
    public void BindRequestHandler(Func<ZRequestContext, CancellationToken, ValueTask> handler) => handlers.Set(handler);

    /// <summary>
    /// Routes a reply back to the request's originating peer. The reply is
    /// consumed; the context stays owned by the REP core and is disposed after
    /// the handler returns.
    /// </summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ZMessage reply, CancellationToken token = default)
    {
        return core.SendReplyAsync(context, reply, token);
    }

    /// <summary>
    /// Routes a reply that borrows the caller's buffer instead of copying
    /// (0026 3.6): zero pool rent, zero copy for <c>byte[]</c>-backed memory
    /// (a non-array backing may be copied inside the awaited write). The caller must not modify
    /// the buffer until the returned task completes (the reply send is
    /// awaited); after the await the buffer is free again.
    /// </summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ReadOnlyMemory<byte> reply, CancellationToken token = default)
    {
        var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed(reply))));
        return core.SendReplyAsync(context, message, token);
    }

    /// <summary>Routes a reply with non-contiguous content, copied, back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ReadOnlySequence<byte> reply, CancellationToken token = default)
    {
        return core.SendReplyAsync(context, ZMessage.Copy(reply), token);
    }

    /// <summary>Routes a multipart reply, copied frame by frame, back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        return core.SendReplyAsync(context, ZMessage.Copy(frames), token);
    }

    /// <summary>Routes a multipart reply from a <c>byte[][]</c> collection, copied, back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, IEnumerable<byte[]> frames, CancellationToken token = default)
    {
        return core.SendReplyAsync(context, ZMessage.Copy(frames), token);
    }
}
