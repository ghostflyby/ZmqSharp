namespace ZmqSharp;

[Flags]
public enum ZmtpFrameFlags : byte
{
    None = 0b0000,

    Last = None,
    More = 0b0001, // message-more
    LongSize = 0b0010, // long-size
    Command = 0b0100, // command-size
}