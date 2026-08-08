namespace ZmqSharp.Sockets;

/// <summary>How a received message is materialized.</summary>
public enum ZReceiveMode
{
    /// <summary>Rent a pooled buffer of exact frame length (default).</summary>
    Pooled,

    /// <summary>Allocate an uninitialized array; never touches a pool.</summary>
    Owned,
}

/// <summary>The complete allocation decision for a message's frames.</summary>
public readonly struct ZReceiveAllocation
{
    /// <summary>Buffer ownership: pooled or owned.</summary>
    public ZReceiveMode Mode { get; init; }

    /// <summary>
    /// True = one contiguous segment per frame; false = chained segments.
    /// Segmented materialization is not implemented yet (0003 open question);
    /// v1 always materializes contiguously.
    /// </summary>
    public bool Contiguous { get; init; }
}

/// <summary>Message-level context passed to the decide hook for its first frame.</summary>
public readonly struct ZReceiveContext
{
    public int FrameLength { get; init; }

    public bool IsFirstFrame { get; init; }

    public bool HasMore { get; init; }
}

/// <summary>Decides, per message, how its frames are allocated.</summary>
public delegate ZReceiveAllocation ZDecide(ZReceiveContext context);

/// <summary>Receive materialization policy for the queue surface (0003).</summary>
public sealed class ZReceiveOptions
{
    public ZReceiveAllocation DefaultAllocation { get; init; } =
        new() { Mode = ZReceiveMode.Pooled, Contiguous = true };

    /// <summary>Frames up to this length stay contiguous; larger frames may be segmented (not yet implemented).</summary>
    public int ContiguousFrameLimit { get; init; } = 85_000;

    /// <summary>When set, evaluated once per message on its first frame; overrides <see cref="DefaultAllocation"/>.</summary>
    public ZDecide? Decide { get; init; }
}
