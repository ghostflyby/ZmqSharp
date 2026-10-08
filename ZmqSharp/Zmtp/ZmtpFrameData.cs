using System.Buffers;

namespace ZmqSharp.Zmtp;

/// <summary>A borrowed logical or wire body. LongSize belongs to the wire header, not this view.</summary>
public readonly struct ZmtpFrameData
{
    public ZmtpFrameFlags Flags { get; init; }
    public ReadOnlySequence<byte> Body { get; init; }
}
