using System.Buffers;
using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>
/// PUB composition root (0013): send-only broadcast. The message's first
/// frame is the topic; every connected peer receives the full message.
/// Non-sealed with an internal core-taking constructor so XPUB can reuse the
/// broadcast send.
/// </summary>
public class ZPubSocket : ZSocketBase
{
    public ZPubSocket(ZSocketOptions options)
        : this(options, new ZPubCore())
    {
    }

    internal ZPubSocket(ZSocketOptions options, ZPubCore core)
        : base(options, core)
    {
    }

    /// <summary>Broadcasts the message (topic = first frame) to every peer; the message is disposed once.</summary>
    public override async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        ThrowIfClosed();
        var peers = PeerSnapshot;
        if (peers.Length == 0)
        {
            message.Dispose();
            return;
        }

        await BroadcastAsync(peers, message, token);
    }
}
