using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Transports;

namespace ZmqSharp.Patterns;

/// <summary>
/// Outbound dispatch for broadcast socket types (PUB, XPUB): selects every
/// connected peer, so the generic send path delivers the full message to all
/// of them (the message's first frame is the topic). With no peer the message
/// is dropped.
/// </summary>
public sealed class ZBroadcastDispatch : IZDispatchPolicy
{
    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        peers.CopyTo(targets);
        return peers.Length;
    }
}

/// <summary>
/// ROUTER identity router (0012): owns the identity-to-connection routing
/// table. The directed identity send
/// <see cref="ZRouterSocket.SendAsync(byte[], ZMessage, CancellationToken)"/>
/// resolves its target through the policy, inbound peers are assigned their
/// routing id by it, and teardown releases the mapping here - the policy is
/// the source of ROUTER's routing and the socket delegates to it. The generic
/// selective send path is rejected: in this API the routing identity is
/// caller-supplied, separate from the message, so the message alone cannot be
/// routed.
/// </summary>
public sealed class ZIdentityDispatch : IZDispatchPolicy
{
    // Identities are matched by content, so a consumer's copy of the identity
    // bytes addresses the peer. Latin1 maps bytes 1:1 to chars (no collisions).
    private readonly Lock identityLock = new();
    private readonly Dictionary<string, IZConnection> identities = [];
    private readonly Dictionary<IZConnection, byte[]> peerIdentities = [];
    private int nextIdentity;

    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        throw new InvalidOperationException("ROUTER sends through SendAsync(identity, message)");
    }

    /// <summary>
    /// Returns the peer's routing identity: the identity it advertised in
    /// READY when one was registered, otherwise a locally assigned id (0025).
    /// </summary>
    internal byte[] AssignIdentity(IZConnection peer)
    {
        lock (identityLock)
        {
            if (peerIdentities.TryGetValue(peer, out var existing)) return existing;

            // Routing ids are local metadata: the identity never leaves this
            // socket, so byte order is irrelevant but fixed for determinism.
            var identity = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(identity, Interlocked.Increment(ref nextIdentity));
            peerIdentities[peer] = identity;
            identities[Encoding.Latin1.GetString(identity)] = peer;
            return identity;
        }
    }

    /// <summary>
    /// Registers the routing identity the peer advertised in READY (0025):
    /// the peer is henceforth addressed by that identity on inbound and
    /// outbound. Returns false when another live peer already claimed it, in
    /// which case the socket refuses the connection (libzmq's ROUTER
    /// duplicate-id behavior); the caller is responsible for rejecting the
    /// peer. A peer registered with an advertised identity is not re-assigned
    /// a local id.
    /// </summary>
    internal bool TryRegisterIdentity(IZConnection peer, ReadOnlyMemory<byte> advertisedIdentity)
    {
        lock (identityLock)
        {
            if (peerIdentities.ContainsKey(peer)) return true;

            var key = Encoding.Latin1.GetString(advertisedIdentity.Span);
            if (identities.TryGetValue(key, out var existing) && !ReferenceEquals(existing, peer))
                return false;

            var identity = advertisedIdentity.ToArray();
            peerIdentities[peer] = identity;
            identities[key] = peer;
            return true;
        }
    }

    /// <summary>Resolves the peer that advertises the given routing identity.</summary>
    internal bool TryResolve(ReadOnlySpan<byte> identity, out IZConnection? peer)
    {
        lock (identityLock)
        {
            return identities.TryGetValue(Encoding.Latin1.GetString(identity), out peer);
        }
    }

    /// <summary>Releases a peer's identity mapping on teardown.</summary>
    internal void RemovePeer(IZConnection peer)
    {
        lock (identityLock)
        {
            if (!peerIdentities.Remove(peer, out var identity)) return;

            identities.Remove(Encoding.Latin1.GetString(identity));
        }
    }
}

/// <summary>
/// REQ's current-connection router (0010): owns the in-flight request target.
/// The request send routes through the policy - <see cref="SelectTargets"/>
/// returns the current connection - so the fair-queue selection made under
/// the in-flight gate is the single source of REQ's routing. The generic send
/// path is rejected when no request is in flight (REQ sends through
/// <see cref="ZReqSocket.RequestAsync(ZMessage, CancellationToken)"/>).
/// </summary>
public sealed class ZCurrentPeerDispatch : IZDispatchPolicy
{
    private readonly Lock gateLock = new();
    private IZConnection? current;

    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        lock (gateLock)
        {
            if (current is null)
                throw new InvalidOperationException("REQ sends through RequestAsync, not SendAsync");

            targets[0] = current;
            return 1;
        }
    }

    /// <summary>True when <paramref name="peer"/> is the in-flight request's target.</summary>
    internal bool IsCurrent(IZConnection peer)
    {
        lock (gateLock) return current == peer;
    }

    /// <summary>Records the in-flight request's target (the fair-queue selection).</summary>
    internal void SetCurrent(IZConnection peer)
    {
        lock (gateLock) current = peer;
    }

    /// <summary>Releases the in-flight target (reply received or peer ended).</summary>
    internal void Clear()
    {
        lock (gateLock) current = null;
    }
}

/// <summary>
/// Outbound dispatch for socket types with no generic selective send:
/// receive-only types (PULL, SUB, XSUB) and the directed-reply type (REP).
/// The generic send path throws with a per-type reason.
/// </summary>
internal sealed class ZNoDispatch(string reason) : IZDispatchPolicy
{
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        throw new InvalidOperationException(reason);
    }
}
