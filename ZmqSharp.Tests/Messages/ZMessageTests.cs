using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;

namespace ZmqSharp.Tests;

public sealed class ZMessageTests
{
    [Fact]
    public void FromOwned_IsSingleFrame_AndAliasesSource()
    {
        byte[] source = [1, 2, 3];
        using var message = ZMessage.FromOwned(source);
        message.FrameCount.Should().Be(1);
        message.GetFrame(0).ToArray().Should().Equal(source);
        message.Payload.ToArray().Should().Equal(source);
        message.TryGetContiguousFrame(0, out var memory).Should().BeTrue();
        memory.ToArray().Should().Equal(source);

        source[1] = 9;
        message.GetFrame(0).FirstSpan[1].Should().Be(9);
    }

    [Fact]
    public void Multipart_FramesAreAccessibleAndPayloadConcatenates()
    {
        using var message = MessageFactory.Multipart("ab"u8.ToArray(), "cde"u8.ToArray());
        message.FrameCount.Should().Be(2);
        message.GetFrame(0).ToArray().Should().Equal("ab"u8.ToArray());
        message.GetFrame(1).ToArray().Should().Equal("cde"u8.ToArray());
        message.Payload.ToArray().Should().Equal("abcde"u8.ToArray());
        message.TryGetContiguousFrame(1, out var memory).Should().BeTrue();
        memory.ToArray().Should().Equal("cde"u8.ToArray());
    }

    [Fact]
    public void SegmentedFrame_IsNotContiguous_ButReadable()
    {
        using var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        byte[] expected = [1, 2, 3, 4, 5];
        message.FrameCount.Should().Be(1);
        message.TryGetContiguousFrame(0, out _).Should().BeFalse();
        message.GetFrame(0).ToArray().Should().Equal(expected);
        message.Payload.ToArray().Should().Equal(expected);
    }

    [Fact]
    public void Dispose_IsIdempotent_AndAccessThrows()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        message.Dispose();
        message.Dispose();

        var act = () => message.GetFrame(0);
        act.Should().Throw<ObjectDisposedException>();
        var actPayload = () => message.Payload;
        actPayload.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void GetFrame_OutOfRange_Throws()
    {
        using var message = MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray());
        var act = () => message.GetFrame(2);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void PooledMultipart_ReturnsBuffersOnDispose()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledMultipart(pool, "a"u8.ToArray(), "b"u8.ToArray());
        message.Dispose();
        pool.Outstanding.Should().Be(0);
    }
}
