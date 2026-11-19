using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// The ZWS flag byte mapping (0035): the ZWS values (0x01 more, 0x02 command)
/// are not the ZMTP flag values, so every conversion is explicit and rejects
/// the combinations the wire cannot represent.
/// </summary>
public sealed class ZwsWireTests
{
    [Theory]
    [InlineData(0x00, ZmtpFrameFlags.None)]
    [InlineData(0x01, ZmtpFrameFlags.More)]
    [InlineData(0x02, ZmtpFrameFlags.Command)]
    public void FromWireByte_MapsLogicalFlags(byte wire, ZmtpFrameFlags expected)
        => Assert.Equal(expected, ZwsWire.FromWireByte(wire));

    [Theory]
    [InlineData((byte)0x03)]
    [InlineData((byte)0x04)]
    [InlineData((byte)0x80)]
    public void FromWireByte_RejectsInvalidFlags(byte wire)
        => Assert.Throws<ZeroMqProtocolException>(() => ZwsWire.FromWireByte(wire));

    [Theory]
    [InlineData(ZmtpFrameFlags.None, (byte)0x00)]
    [InlineData(ZmtpFrameFlags.More, (byte)0x01)]
    [InlineData(ZmtpFrameFlags.Command, (byte)0x02)]
    public void ToWireByte_MapsLogicalFlags(ZmtpFrameFlags flags, byte expected)
        => Assert.Equal(expected, ZwsWire.ToWireByte(flags));

    [Fact]
    public void ToWireByte_RejectsCommandWithMore()
        => Assert.Throws<ZeroMqProtocolException>(
            () => ZwsWire.ToWireByte(ZmtpFrameFlags.Command | ZmtpFrameFlags.More));
}
