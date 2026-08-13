using ZmqSharp.Transports;

namespace ZmqSharp.Patterns;

/// <summary>
/// Single-peer outbound selection: a message always goes to the first (only)
/// connection; with no peer the message is dropped. Selects zero or one
/// target. Serves PAIR.
/// </summary>
public sealed class ZSinglePeerDispatch : IZDispatchPolicy
{
    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        if (peers.IsEmpty) return 0;

        targets[0] = peers[0];
        return 1;
    }
}
