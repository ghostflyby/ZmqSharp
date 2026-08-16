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
    private Func<ZRequestContext, CancellationToken, ValueTask>? requestHandler;

    public ZRepSocket(ZSocketOptions? options = null)
        : this(options ?? new ZSocketOptions(), new ZRepCore())
    {
    }

    private ZRepSocket(ZSocketOptions options, ZRepCore core)
        : base(options, new ZNoDispatch("REP replies through SendReplyAsync, not SendAsync"), ZSocketTypes.Rep, core)
    {
        this.core = core;
        core.Attach(this);
    }

    /// <summary>
    /// Binds the request handler. Requests are delivered one at a time across
    /// all peers; awaiting the handler holds the slot, so a slow handler
    /// backpressures the receiving pumps. The context is valid only during the
    /// call; reply with <see cref="SendReplyAsync"/>.
    /// </summary>
    public void BindRequestHandler(Func<ZRequestContext, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (StateLock)
        {
            requestHandler = handler;
        }
    }

    /// <summary>
    /// Routes a reply back to the request's originating peer. The reply is
    /// consumed; the context stays owned by the REP core and is disposed after
    /// the handler returns.
    /// </summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ZMessage reply, CancellationToken token = default)
    {
        return core.SendReplyAsync(this, context, reply, token);
    }

    /// <summary>Routes a reply copied into an owned buffer back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ReadOnlyMemory<byte> reply, CancellationToken token = default)
    {
        return core.SendReplyAsync(this, context, ZMessage.Copy(reply), token);
    }

    /// <summary>Routes a reply with non-contiguous content, copied, back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, ReadOnlySequence<byte> reply, CancellationToken token = default)
    {
        return core.SendReplyAsync(this, context, ZMessage.Copy(reply), token);
    }

    /// <summary>Routes a multipart reply, copied frame by frame, back to the originating peer (0026).</summary>
    public ValueTask SendReplyAsync(ZRequestContext context, IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        return core.SendReplyAsync(this, context, ZMessage.Copy(frames), token);
    }

    internal ValueTask RaiseRequestAsync(ZRequestContext context, CancellationToken token)
    {
        Func<ZRequestContext, CancellationToken, ValueTask>? handler;
        lock (StateLock)
        {
            handler = requestHandler;
        }

        return handler?.Invoke(context, token) ?? ValueTask.CompletedTask;
    }
}
