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

/// <summary>Why a received frame was rejected by the connection-level guard.</summary>
public enum ZReceiveRejectionReason
{
    /// <summary>The frame exceeds the configured single-frame limit.</summary>
    FrameTooLarge,

    /// <summary>The accumulated message exceeds the configured total limit.</summary>
    MessageTooLarge,

    /// <summary>The message has more frames than the configured per-message limit.</summary>
    TooManyFrames,
}

/// <summary>The rejection payload of the connection-level guard (0008 D1).</summary>
public readonly struct ZReceiveRejection
{
    /// <summary>Classification of the rejection.</summary>
    public ZReceiveRejectionReason Reason { get; init; }

    /// <summary>The configured limit, when the rejection is a numeric-limit violation.</summary>
    public long? Limit { get; init; }

    /// <summary>The observed value, when the rejection is a numeric-limit violation.</summary>
    public long? Actual { get; init; }
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

/// <summary>
/// Internal signal that the connection-level guard rejected a frame.
/// Propagates as the connection failure through the existing teardown path; no
/// wire ERROR is sent for a traffic-phase rejection (0008 D5).
/// </summary>
internal sealed class ZReceiveRejectedException(ZReceiveRejection rejection) : Exception
{
    public ZReceiveRejection Rejection { get; } = rejection;
}

/// <summary>Checked message-total accounting for the receive pipeline (0008 D3/D6).</summary>
internal static class ZReceiveGuard
{
    /// <summary>
    /// Adds a frame length to the running message total with checked
    /// arithmetic. Overflow reports failure instead of throwing, so an
    /// unrepresentable total surfaces as a MessageTooLarge rejection rather
    /// than an arithmetic exception.
    /// </summary>
    public static bool TryAccumulate(long current, int length, out long total)
    {
        try
        {
            total = checked(current + length);
            return true;
        }
        catch (OverflowException)
        {
            total = 0;
            return false;
        }
    }

    /// <summary>
    /// Checks the connection-level receive limits for one frame in the fixed
    /// order frame, message total, frames per message (0008 D4). Returns null
    /// when the frame passes every limit.
    /// </summary>
    public static ZReceiveRejection? CheckLimits(
        int frameLength,
        long accumulatedLength,
        int frameIndex,
        long maxFrameLength,
        long maxMessageLength,
        int maxFramesPerMessage)
    {
        if (frameLength > maxFrameLength)
        {
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.FrameTooLarge,
                Limit = maxFrameLength,
                Actual = frameLength,
            };
        }

        if (accumulatedLength > maxMessageLength)
        {
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.MessageTooLarge,
                Limit = maxMessageLength,
                Actual = accumulatedLength,
            };
        }

        if (frameIndex >= maxFramesPerMessage)
        {
            // FrameIndex is zero-based, so reaching the limit means a frame is already in excess.
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.TooManyFrames,
                Limit = maxFramesPerMessage,
                Actual = frameIndex + 1L,
            };
        }

        return null;
    }
}
