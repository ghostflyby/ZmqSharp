using ZmqSharp.Transports;

namespace ZmqSharp.Patterns;

/// <summary>
/// Round-robin outbound selection: each call advances a fair-queue cursor, so
/// a slow or failing peer never starves the others (0010 section 2). The
/// cursor advances unconditionally regardless of the message. Selects exactly
/// one target. Serves DEALER, PUSH, and REQ's fair-queue send across
/// connections.
/// </summary>
public sealed class ZRoundRobinDispatch : IZDispatchPolicy
{
    private int next;

    /// <inheritdoc/>
    public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
    {
        if (peers.IsEmpty) return 0;

        var index = (Interlocked.Increment(ref next) - 1) % peers.Length;
        targets[0] = peers[index];
        return 1;
    }
}
