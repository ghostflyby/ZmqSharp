using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS frame flag byte (RFC 45 / libzmq ws_protocol.hpp): 0x00 final,
/// 0x01 more, 0x02 command. These are NOT the ZMTP 3.0 flag values (where
/// 0x02 is the long-size marker and 0x04 is command) - the byte position is
/// shared but the encoding differs, so every boundary converts explicitly
/// (0035).
/// </summary>
internal static class ZwsWire
{
    public const byte More = 0x01;
    public const byte Command = 0x02;

    /// <summary>Logical ZMTP flags to the ZWS wire byte. Flags must be validated first.</summary>
    public static byte ToWireByte(ZmtpFrameFlags flags)
    {
        ZWireFrameLogic.Validate(flags);
        if ((flags & ZmtpFrameFlags.Command) != 0) return Command;
        return (flags & ZmtpFrameFlags.More) != 0 ? More : (byte)0;
    }

    /// <summary>The ZWS wire byte to logical ZMTP flags; reserved bits and the command+more combination are violations.</summary>
    public static ZmtpFrameFlags FromWireByte(byte wire)
    {
        if ((wire & ~(More | Command)) != 0)
            throw new ZeroMqProtocolException("reserved ZWS frame flag bits are set");
        if (wire == (More | Command))
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var flags = ZmtpFrameFlags.None;
        if ((wire & More) != 0) flags |= ZmtpFrameFlags.More;
        if ((wire & Command) != 0) flags |= ZmtpFrameFlags.Command;
        return flags;
    }
}
