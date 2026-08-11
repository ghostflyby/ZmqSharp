using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// ROUTER composition root (0012): identity-aware routing. Inbound messages
/// arrive with the peer's routing identity prefixed as the first frame
/// (consumers bind their own <see cref="IPatternSink"/> or wrap this in the
/// channel surface); <see cref="SendAsync(byte[], ZMessage, CancellationToken)"/>
/// addresses a peer by identity. Wire frames carry no identity (ZMTP 3.0
/// routing ids are local to the router).
/// </summary>
public sealed class ZRouterSocket : ZSocketBase
{
    // Identities are matched by content, so a consumer's copy of the identity
    // bytes addresses the peer. Latin1 maps bytes 1:1 to chars (no collisions).
    private readonly Dictionary<string, IZConnection> identities = [];
    private readonly Dictionary<IZConnection, byte[]> peerIdentities = [];
    private int nextIdentity;

    public ZRouterSocket(ZSocketOptions options)
        : this(options, new ZRouterCore())
    {
    }

    private ZRouterSocket(ZSocketOptions options, ZRouterCore core)
        : base(options, core)
    {
        // Identity mapping lives with the peer; release it on teardown so a
        // long-lived ROUTER never retains disposed connections or stale ids
        // (subagent review finding).
        PeerEnded += (peer, _) => RemoveIdentity(peer);
    }

    /// <summary>
    /// Sends to the peer identified by <paramref name="identity"/>; an unknown
    /// identity drops the message (libzmq ROUTER default). The message is
    /// disposed after the send.
    /// </summary>
    public async ValueTask SendAsync(byte[] identity, ZMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(identity);
        IZConnection? peer;
        lock (StateLock)
        {
            identities.TryGetValue(Encoding.Latin1.GetString(identity), out peer);
        }

        if (peer is null)
        {
            message.Dispose();
            return;
        }

        await SendToAsync(peer, message, token);
    }

    public async ValueTask SendAsync(byte[] identity, ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        var owner = Pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var message = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, bytes.Length))));
        await SendAsync(identity, message, token);
    }

    /// <summary>Prefixes the peer's routing identity to every inbound message.</summary>
    protected override ZMessage? PrepareInboundForSink(IZConnection peer, ZMessage message)
    {
        var identity = GetOrAssignIdentity(peer);
        var frames = new List<ZFrame>(message.Count + 1)
        {
            new(new ZSegment(identity, 0, identity.Length)),
        };
        for (var i = 0; i < message.Count; i++)
        {
            frames.Add(message[i]);
        }

        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    private byte[] GetOrAssignIdentity(IZConnection peer)
    {
        lock (StateLock)
        {
            if (peerIdentities.TryGetValue(peer, out var existing))
            {
                return existing;
            }

            // Routing ids are local metadata: the identity never leaves this
            // socket, so byte order is irrelevant but fixed for determinism.
            var identity = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(identity, Interlocked.Increment(ref nextIdentity));
            peerIdentities[peer] = identity;
            identities[Encoding.Latin1.GetString(identity)] = peer;
            return identity;
        }
    }

    private void RemoveIdentity(IZConnection peer)
    {
        lock (StateLock)
        {
            if (!peerIdentities.Remove(peer, out var identity))
            {
                return;
            }

            identities.Remove(Encoding.Latin1.GetString(identity));
        }
    }
}
