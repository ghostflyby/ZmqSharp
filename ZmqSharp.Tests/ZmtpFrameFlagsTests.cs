using FluentAssertions;
using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

public sealed class ZmtpFrameFlagsTests
{
    [Fact]
    public void MoreBit_IsSet()
    {
        ((byte)ZmtpFrameFlags.More).Should().Be(0b0001);
    }

    [Fact]
    public void LongSizeBit_IsSet()
    {
        ((byte)ZmtpFrameFlags.LongSize).Should().Be(0b0010);
    }
}
