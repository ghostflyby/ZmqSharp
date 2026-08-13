using ZmqSharp.Transports;

namespace ZmqSharp.Patterns;

/// <summary>
/// Outbound selection only: decides which connections receive a message, or
/// none at all to drop it (0015 section 2.1). A neutral, reusable policy - it
/// knows nothing about the socket type that hosts it, so the same policy can
/// serve several socket types (e.g. round-robin for DEALER and PUSH, or the
/// select-all policy for PUB and XPUB).
/// <para>
/// The policy is the primary decision maker on the selective send path: the
/// socket hands it the routable peer set, and it selects zero or more targets
/// that the socket then sends to, exactly once each. A policy selects more
/// than one target only when every selected peer must receive the message
/// (broadcast). Caller-addressed sends (ROUTER identity addressing, REP
/// replies) resolve their target from per-call context and do not consult the
/// policy.
/// </para>
/// </summary>
public interface IZDispatchPolicy
{
    /// <summary>
    /// Selects the connections that receive <paramref name="message"/>,
    /// writing them to <paramref name="targets"/> (capacity:
    /// <c>peers.Length</c>) and returning how many were selected. Zero = drop
    /// the message. The socket sends to exactly the selected peers, in order,
    /// once each.
    /// </summary>
    int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets);
}
