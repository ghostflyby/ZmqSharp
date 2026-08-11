using System.Threading;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Per-pattern wire semantics, composed with the transport core rather than
/// subclassed (0007 section 2.2): outbound selection and the advertised
/// Socket-Type. The transport core owns transport mechanics only; a socket
/// type is a thin composition root binding one core to the base.
/// </summary>
internal interface IPatternCore
{
    /// <summary>Selects the outbound connection for a message; null = drop.</summary>
    IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers);

    /// <summary>ZMTP Socket-Type metadata advertised in the READY handshake.</summary>
    string SocketTypeName { get; }
}

/// <summary>PAIR semantics: single peer, no routing.</summary>
internal sealed class ZPairCore : IPatternCore
{
    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
        => peers.IsEmpty ? null : peers[0];

    public string SocketTypeName => "PAIR";
}

/// <summary>DEALER semantics: fair dispatch, round-robin outbound.</summary>
internal sealed class ZDealerCore : IPatternCore
{
    private int next;

    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
    {
        if (peers.IsEmpty)
        {
            return null;
        }

        int index = (Interlocked.Increment(ref next) - 1) % peers.Length;
        return peers[index];
    }

    public string SocketTypeName => "DEALER";
}

/// <summary>
/// REQ semantics (libzmq, 0010): strict single in-flight request, round-robin
/// outbound, replies accepted only from the current peer.
/// </summary>
internal sealed class ZReqCore : IPatternCore
{
    private readonly Lock gateLock = new();
    private int next;
    private IZConnection? current;
    private TaskCompletionSource<ZMessage>? pending;

    public string SocketTypeName => "REQ";

    /// <summary>
    /// The generic base send path is invalid for REQ: sends go through
    /// <c>RequestAsync</c>, which manages the in-flight gate atomically.
    /// </summary>
    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
        => throw new InvalidOperationException("REQ sends through RequestAsync, not SendAsync");

    public Task<ZMessage> RequestAsync(ZReqSocket socket, ZMessage message, CancellationToken token)
    {
        Task<ZMessage> result;
        lock (gateLock)
        {
            if (pending is not null)
            {
                throw new InvalidOperationException("a request is already in flight");
            }

            var peers = socket.PeerSnapshot;
            if (peers.Length == 0)
            {
                throw new InvalidOperationException("no connected peer to send the request to");
            }

            // Round robin: the cursor advances unconditionally, so a slow or
            // failing peer never starves the others (0010 section 2).
            current = peers[(Interlocked.Increment(ref next) - 1) % peers.Length];
            pending = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            result = pending.Task;
        }

        _ = SendFramedAsync(socket, message, token);
        return result;
    }

    public ValueTask OnMessageAsync(ZReqSocket socket, IZConnection peer, ZMessage message, CancellationToken token)
    {
        TaskCompletionSource<ZMessage>? completion;
        lock (gateLock)
        {
            if (peer != current || pending is null)
            {
                completion = null;
            }
            else
            {
                completion = pending;
                current = null;
                pending = null;
            }
        }

        if (completion is null)
        {
            // Not the current peer's reply (out-of-order / spurious): discard.
            message.Dispose();
            return ValueTask.CompletedTask;
        }

        var reply = InterpretInbound(message);
        completion.TrySetResult(reply);
        return ValueTask.CompletedTask;
    }

    public void OnPeerEnded(IZConnection peer)
    {
        TaskCompletionSource<ZMessage>? completion;
        lock (gateLock)
        {
            if (peer != current || pending is null)
            {
                return;
            }

            completion = pending;
            current = null;
            pending = null;
        }

        completion.TrySetException(new IOException("peer closed before the reply arrived"));
    }

    private async Task SendFramedAsync(ZReqSocket socket, ZMessage message, CancellationToken token)
    {
        var framed = BuildOutbound(message);
        try
        {
            await socket.SendToAsync(current!, framed, token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A transport-level failure (the peer retired mid-send) faults the
            // pending request and frees the in-flight gate.
            TaskCompletionSource<ZMessage>? completion;
            lock (gateLock)
            {
                completion = pending;
                current = null;
                pending = null;
            }

            completion?.TrySetException(ex);
        }
    }

    /// <summary>Appends the empty delimiter frame; frames move (0007 M3).</summary>
    private static ZMessage BuildOutbound(ZMessage message)
    {
        var frames = new List<ZFrame>(message.Count + 1);
        for (var i = 0; i < message.Count; i++)
        {
            frames.Add(message[i]);
        }

        frames.Add(EmptyFrame);
        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    /// <summary>Strips the trailing empty delimiter; the wire message's frames move.</summary>
    private static ZMessage InterpretInbound(ZMessage message)
    {
        var count = message.Count;
        if (count < 2 || message[count - 1].ToSequence().Length != 0)
        {
            message.Dispose();
            throw new ZeroMqProtocolException("reply is missing the trailing empty delimiter");
        }

        var frames = new List<ZFrame>(count - 1);
        for (var i = 0; i < count - 1; i++)
        {
            frames.Add(message[i]);
        }

        return frames.Count == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage([.. frames]));
    }

    internal static readonly ZFrame EmptyFrame = new(new ZSegment(Array.Empty<byte>(), 0, 0));
}

/// <summary>
/// REP semantics (libzmq, 0010): fair intake of requests serialized across
/// peers, directed replies routed back to the originating peer, strict
/// request/reply alternation.
/// </summary>
internal sealed class ZRepCore : IPatternCore
{
    private readonly SemaphoreSlim slot = new(1, 1);

    public string SocketTypeName => "REP";

    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
        => throw new InvalidOperationException("REP replies through SendReplyAsync, not SendAsync");

    public async ValueTask OnMessageAsync(ZRepSocket socket, IZConnection peer, ZMessage message, CancellationToken token)
    {
        // Strict alternation across all peers: one request is handled at a
        // time; the per-peer pumps stay alive (no starvation, 0010 section 3).
        await slot.WaitAsync(token);
        ZRequestContext? context = null;
        try
        {
            context = new ZRequestContext(peer, InterpretInbound(message));
            await socket.RaiseRequestAsync(context.Value, token);
        }
        finally
        {
            context?.Dispose();
            slot.Release();
        }
    }

    public ValueTask SendReplyAsync(ZRepSocket socket, ZRequestContext context, ZMessage reply, CancellationToken token)
    {
        var framed = BuildOutbound(reply);
        return socket.SendToAsync(context.Peer, framed, token);
    }

    private static ZMessage BuildOutbound(ZMessage message)
    {
        var frames = new List<ZFrame>(message.Count + 1);
        for (var i = 0; i < message.Count; i++)
        {
            frames.Add(message[i]);
        }

        frames.Add(ZReqCore.EmptyFrame);
        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    private static ZMessage InterpretInbound(ZMessage message)
    {
        var count = message.Count;
        if (count < 2 || message[count - 1].ToSequence().Length != 0)
        {
            message.Dispose();
            throw new ZeroMqProtocolException("request is missing the trailing empty delimiter");
        }

        var frames = new List<ZFrame>(count - 1);
        for (var i = 0; i < count - 1; i++)
        {
            frames.Add(message[i]);
        }

        return frames.Count == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage([.. frames]));
    }
}
