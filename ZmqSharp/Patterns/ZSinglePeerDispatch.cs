namespace ZmqSharp.Patterns;

/// <summary>
/// Single-peer outbound selection: a message always goes to the first (only)
/// connection; with no peer the message is dropped. Selects zero or one
/// target. Serves PAIR.
/// </summary>
public sealed class ZSinglePeerDispatch : IZDispatchPolicy
{
    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<ZPeer> peers, Span<ZPeer> targets)
    {
        if (peers.IsEmpty) return 0;

        targets[0] = peers[0];
        return 1;
    }
}
