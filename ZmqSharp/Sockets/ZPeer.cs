namespace ZmqSharp;

/// <summary>Stable local peer identity. Reconnection creates a new identity; this is not a wire routing id.</summary>
public sealed class ZPeer
{
    private static long nextId;
    internal ZPeer() => Id = Interlocked.Increment(ref nextId);
    public long Id { get; }
}
