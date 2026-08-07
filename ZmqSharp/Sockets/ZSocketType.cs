namespace ZmqSharp.Sockets;

/// <summary>Socket types; each maps to a scheduling policy.</summary>
public enum ZSocketType
{
    /// <summary>Single peer, no routing (PAIR semantics).</summary>
    Pair,

    /// <summary>Fair dispatch: round-robin outbound, fair-queue inbound (DEALER semantics).</summary>
    Dealer,
}
