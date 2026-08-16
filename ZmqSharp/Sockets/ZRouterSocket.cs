using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp;

/// <summary>
/// ROUTER composition root (0012): identity-aware routing. Inbound messages
/// arrive with the peer's routing identity prefixed as the first frame
/// (consumers bind their own <see cref="IPatternSink"/> or wrap this in the
/// channel surface); <see cref="SendAsync(byte[], ZMessage, CancellationToken)"/>
/// addresses a peer by identity. The routing table and the inbound identity
/// prefix both live in the composed <see cref="ZIdentityDispatch"/> / its
/// inbound policy; this socket only delegates. Wire frames carry no identity
/// (ZMTP 3.0 routing ids are local to the router).
/// </summary>
public sealed class ZRouterSocket : ZQueueSocketBase
{
    private readonly ZIdentityDispatch dispatch;

    public ZRouterSocket(ZSocketOptions? options = null)
        : this(options ?? new ZSocketOptions(), new ZIdentityDispatch())
    {
    }

    private ZRouterSocket(ZSocketOptions options, ZIdentityDispatch dispatch)
        : base(options, dispatch, ZSocketTypes.Router, new RouterInboundPolicy(dispatch))
    {
        this.dispatch = dispatch;
        // The identity mapping lives in the routing policy; release it on
        // teardown so a long-lived ROUTER never retains disposed connections
        // or stale ids (subagent review finding).
        PeerEnded += (peer, _) => dispatch.RemovePeer(peer);
    }

    /// <summary>
    /// Sends to the peer identified by <paramref name="identity"/>; an unknown
    /// identity drops the message (libzmq ROUTER default). The message is
    /// disposed after the send.
    /// </summary>
    public async ValueTask SendAsync(ReadOnlyMemory<byte> identity, ZMessage message, CancellationToken token = default)
    {
        if (identity.IsEmpty || !dispatch.TryResolve(identity.Span, out var peer) || peer is null)
        {
            message.Dispose();
            return;
        }

        await SendToAsync(peer, message, token);
    }

    /// <summary>
    /// Identity-addressed send that borrows the caller's buffer instead of
    /// copying (0026 3.6): zero-copy, no pool rent, for any
    /// <see cref="ReadOnlyMemory{T}"/> backing. The caller must not modify
    /// the buffer until the returned task completes; after the await the
    /// buffer is free again.
    /// </summary>
    public async ValueTask SendAsync(ReadOnlyMemory<byte> identity, ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        if (identity.IsEmpty || !dispatch.TryResolve(identity.Span, out var peer) || peer is null)
            return;

        var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed(bytes))));
        await SendToAsync(peer, message, token);
    }

    /// <summary>Addresses a peer by identity with a non-contiguous frame, copied (0026).</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> identity, ReadOnlySequence<byte> frame, CancellationToken token = default)
    {
        return SendAsync(identity, ZMessage.Copy(frame), token);
    }

    /// <summary>Addresses a peer by identity with a multipart message, copied frame by frame (0026).</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> identity, IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default)
    {
        return SendAsync(identity, ZMessage.Copy(frames), token);
    }

    /// <summary>Addresses a peer by identity with a multipart message from a <c>byte[][]</c> collection, copied (0026).</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> identity, IEnumerable<byte[]> frames, CancellationToken token = default)
    {
        return SendAsync(identity, ZMessage.Copy(frames), token);
    }

    /// <summary>
    /// Registers the peer's advertised READY identity in the routing dispatch
    /// (0025). A peer that advertised none keeps the local assignment. A
    /// second peer claiming an in-use identity is refused at establishment -
    /// the libzmq ROUTER duplicate-id behavior (mechanism/router sources).
    /// </summary>
    protected override void OnPeerEstablished(IZConnection peer, ReadOnlyMemory<byte>? advertisedIdentity)
    {
        if (advertisedIdentity is not { Length: > 0 } identity) return;

        if (!dispatch.TryRegisterIdentity(peer, identity))
            throw new ZeroMqProtocolException("peer advertises an in-use routing identity");
    }

    /// <summary>
    /// ROUTER's inbound policy (0019): assigns the peer its routing identity
    /// through the routing dispatch and delivers the message prefixed with it.
    /// </summary>
    private sealed class RouterInboundPolicy(ZIdentityDispatch dispatch) : IZInboundPolicy
    {
        public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            var identity = dispatch.AssignIdentity(peer);
            var frames = new List<ZFrame>(message.Count + 1)
            {
                new(new ZSegment(identity, 0, identity.Length))
            };
            for (var i = 0; i < message.Count; i++) frames.Add(message[i]);

            return ValueTask.FromResult(ZInboundDecision.Deliver(
                new ZMessage(new ZMultiMessage([.. frames]))));
        }
    }
}
