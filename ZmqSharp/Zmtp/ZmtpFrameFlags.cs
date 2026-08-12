namespace ZmqSharp.Zmtp;

/// <summary>
/// ZMTP 3.0 frame flags (wire header byte). Public because a session
/// connection implementing <see cref="ZmqSharp.Transports.IZConnection"/>
/// (e.g. a CURVE encrypting wrapper) reads and reproduces these flags on the
/// frames it serves.
/// </summary>
[Flags]
public enum ZmtpFrameFlags : byte
{
    None = 0b0000,
    More = 0b0001, // message-more
    LongSize = 0b0010, // long-size
    Command = 0b0100, // command-size
}
