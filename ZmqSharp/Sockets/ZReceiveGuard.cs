namespace ZmqSharp.Sockets;

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
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.FrameTooLarge,
                Limit = maxFrameLength,
                Actual = frameLength
            };

        if (accumulatedLength > maxMessageLength)
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.MessageTooLarge,
                Limit = maxMessageLength,
                Actual = accumulatedLength
            };

        if (frameIndex >= maxFramesPerMessage)
            // FrameIndex is zero-based, so reaching the limit means a frame is already in excess.
            return new ZReceiveRejection
            {
                Reason = ZReceiveRejectionReason.TooManyFrames,
                Limit = maxFramesPerMessage,
                Actual = frameIndex + 1L
            };

        return null;
    }
}
