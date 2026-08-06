using System.Buffers;

namespace ZmqSharp.Zmtp;

/// <summary>Cumulative information visible at the decision point.</summary>
public readonly struct ZReceiveContext(
    ReadOnlySequence<byte> firstFrame,
    long bytesSeen,
    long nextFrameSize,
    int framesSeen)
{
    /// <summary>
    /// First frame (an application protocol can read a command/type); empty when
    /// the decision runs before the first frame body is available.
    /// </summary>
    public ReadOnlySequence<byte> FirstFrame { get; } = firstFrame;

    /// <summary>Bytes materialized so far for the current message.</summary>
    public long BytesSeen { get; } = bytesSeen;

    /// <summary>Known length of the current frame from its header.</summary>
    public long NextFrameSize { get; } = nextFrameSize;

    /// <summary>Frames seen so far.</summary>
    public int FramesSeen { get; } = framesSeen;
}
