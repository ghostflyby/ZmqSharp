using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// REP coordination (0010 section 4; 0015 section 2.1; 0019): fair intake of
/// requests serialized across peers (one at a time, strict alternation),
/// directed replies routed back to the originating peer. The inbound side is
/// the consume arm of the inbound seam (0019): the core is the socket's
/// <see cref="IZInboundPolicy"/>, waiting on the request slot and raising the
/// request handler. Delimiter framing is <see cref="ZDelimiterFraming"/>.
/// </summary>
internal sealed class ZRepCore : IZInboundPolicy
{
    private readonly SemaphoreSlim slot = new(1, 1);
    private ZRepSocket? socket;

    /// <summary>Binds the owning socket (after the base constructor completes) so
    /// the consume path can raise its request handler.</summary>
    internal void Attach(ZRepSocket socket)
    {
        this.socket = socket;
    }

    public async ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        // Strict alternation across all peers: one request is handled at a
        // time; the per-peer pumps stay alive (no starvation, 0010 section 3).
        // A cancelled slot wait must still release the aggregated message.
        try
        {
            await slot.WaitAsync(token);
        }
        catch (OperationCanceledException)
        {
            message.Dispose();
            throw;
        }

        ZRequestContext? context = null;
        try
        {
            context = new ZRequestContext(peer, ZDelimiterFraming.Decode(message, "reply"));
            if (socket is { } target)
                await target.RaiseRequestAsync(context.Value, token);
        }
        finally
        {
            context?.Dispose();
            slot.Release();
        }

        return ZInboundDecision.Consumed();
    }

    public ValueTask SendReplyAsync(ZRepSocket socket, ZRequestContext context, ZMessage reply, CancellationToken token)
    {
        var framed = ZDelimiterFraming.Encode(reply);
        return socket.SendToAsync(context.Peer, framed, token);
    }
}
