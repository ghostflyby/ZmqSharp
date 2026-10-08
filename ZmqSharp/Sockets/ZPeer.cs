namespace ZmqSharp;

/// <summary>Stable local peer identity. Reconnection creates a new identity; this is not a wire routing id.</summary>
public sealed class ZPeer
{
    private static long _nextId;
    internal ZPeer() => Id = Interlocked.Increment(ref _nextId);
    public long Id { get; }
}
