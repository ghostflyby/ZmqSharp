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
    /// True = chained segments; false (default) = one contiguous segment.
    /// Segmented materialization is a 0003 open question; v1 always
    /// materializes contiguously.
    /// </summary>
    public bool Segmented { get; init; }

    public bool Contiguous => !Segmented;
}

/// <summary>Frame context plus message accumulation passed to the decide hook.</summary>
public readonly struct ZReceiveContext
{
    /// <summary>Length of the current frame.</summary>
    public int FrameLength { get; init; }

    /// <summary>True when more frames of the same message follow.</summary>
    public bool HasMore { get; init; }

    /// <summary>Zero-based index of the current frame within its message.</summary>
    public int FrameIndex { get; init; }

    /// <summary>Total bytes accumulated in the message up to and including this frame.</summary>
    public long AccumulatedLength { get; init; }

    /// <summary>True when this frame starts a message.</summary>
    public bool IsFirstFrame => FrameIndex == 0;
}

/// <summary>
/// Decides how each frame is allocated. Allocation only: the resource limits
/// are enforced by a connection-level guard outside this policy, so a policy
/// never decides whether a frame may be received (0008 D1).
/// </summary>
public interface IZReceivePolicy
{
    ZReceiveAllocation Decide(ZReceiveContext context);
}

/// <summary>Wraps a decide delegate as a policy.</summary>
public sealed class ZDelegateReceivePolicy(ZDecide decide) : IZReceivePolicy
{
    public ZReceiveAllocation Decide(ZReceiveContext context) => decide(context);
}

/// <summary>Decides how each frame is allocated, with message accumulation context.</summary>
public delegate ZReceiveAllocation ZDecide(ZReceiveContext context);

/// <summary>
/// Numeric-configuration receive policy: fixed ownership plus a frame-length
/// threshold that decides continuity. Allocation only; rejection limits are
/// not part of the policy - they are enforced by a connection-level guard on
/// <see cref="ZQueueSocketOptions"/> (0008 D1/D6), so a custom policy can
/// never bypass them.
/// </summary>
public sealed class ZReceiveOptions : IZReceivePolicy
{
    public ZReceiveMode Mode { get; init; } = ZReceiveMode.Pooled;

    /// <summary>Frames longer than this materialize segmented; at or below, contiguous.</summary>
    public int ContiguousFrameLimit { get; init; } = 85_000;

    public ZReceiveAllocation Decide(ZReceiveContext context)
        => new()
        {
            Mode = Mode,
            Segmented = context.FrameLength > ContiguousFrameLimit,
        };
}
