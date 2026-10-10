using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

public sealed class ZmtpFrameFlagsTests
{
    [Fact]
    public void MoreBit_IsSet()
    {
        Assert.Equal(0b0001, (byte)ZmtpFrameFlags.More);
    }

    [Fact]
    public void LongSizeBit_IsSet()
    {
        Assert.Equal(0b0010, (byte)ZmtpFrameFlags.LongSize);
    }
}
