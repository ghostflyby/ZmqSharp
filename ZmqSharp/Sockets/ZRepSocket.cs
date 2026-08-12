using ZmqSharp.Sockets;
using ZmqSharp.Transports;
namespace ZmqSharp;

/// <summary>
/// REP composition root (0010 section 4): fair intake of requests serialized
/// across peers (one at a time, strict alternation), delivered as a
/// <see cref="ZRequestContext"/>; replies route back to the originating peer.
/// </summary>
public sealed class ZRepSocket : ZSocketBase, IPatternSink
{
    private readonly ZRepCore core;
    private Func<ZRequestContext, CancellationToken, ValueTask>? requestHandler;

    public ZRepSocket(ZSocketOptions options)
        : base(options, new ZRepCore())
    {
        core = (ZRepCore)Core;
        BindMessageSink(this);
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

    internal ValueTask RaiseRequestAsync(ZRequestContext context, CancellationToken token)
    {
        Func<ZRequestContext, CancellationToken, ValueTask>? handler;
        lock (StateLock)
        {
            handler = requestHandler;
        }

        return handler?.Invoke(context, token) ?? ValueTask.CompletedTask;
    }

    public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        return core.OnMessageAsync(this, peer, message, token);
    }
}
