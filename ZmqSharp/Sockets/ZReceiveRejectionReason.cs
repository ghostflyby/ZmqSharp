namespace ZmqSharp;

/// <summary>Why a received frame was rejected by the connection-level guard.</summary>
public enum ZReceiveRejectionReason
{
    /// <summary>The frame exceeds the configured single-frame limit.</summary>
    FrameTooLarge,

    /// <summary>The accumulated message exceeds the configured total limit.</summary>
    MessageTooLarge,

    /// <summary>The message has more frames than the configured per-message limit.</summary>
    TooManyFrames
}
