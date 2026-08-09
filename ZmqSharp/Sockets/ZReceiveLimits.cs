namespace ZmqSharp.Sockets;

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
