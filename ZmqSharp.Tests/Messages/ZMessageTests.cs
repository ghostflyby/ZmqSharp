using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;

namespace ZmqSharp.Tests.Messages;

public sealed class ZMessageTests
{
    [Fact]
    public void FromOwned_IsSingleFrame_AndAliasesSource()
    {
        byte[] source = [1, 2, 3];
        using var message = ZMessage.FromOwned(source);
        message.FrameCount.Should().Be(1);
        message.GetOrigin(0).Should().Be(ZBufferOrigin.Owned);
        message.GetFrame(0).ToArray().Should().Equal(source);
        message.Whole.ToArray().Should().Equal(source);

        source[1] = 9;
        message.GetFrame(0).FirstSpan[1].Should().Be(9);
    }

    [Fact]
    public void ToOwnedArray_IsIndependentCopy()
    {
        byte[] source = [1, 2, 3];
        using var message = ZMessage.FromOwned(source);
        var copy = message.ToOwnedArray();
        copy.Should().Equal(source);

        copy[0] = 42;
        message.GetFrame(0).FirstSpan[0].Should().Be(1);
    }

    [Fact]
    public void ToOwnedArray_Multipart_ConcatenatesInOrder()
    {
        using var message = MessageFactory.Multipart("ab"u8.ToArray(), "cde"u8.ToArray());
        message.ToOwnedArray().Should().Equal("abcde"u8.ToArray());
    }

    [Fact]
    public void TryTakeOwner_SingleFramePooled_TransfersAndMessageDoesNotReturn()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledSingleFrame(pool, "hello"u8.ToArray());

        message.TryTakeOwner(out var owner).Should().BeTrue();
        var taken = owner ?? throw new InvalidOperationException("expected an owner");

        message.Dispose();
        pool.Outstanding.Should().Be(1);
        taken.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void TryTakeOwner_Multipart_Rejects()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledMultipart(pool, "a"u8.ToArray(), "b"u8.ToArray());

        message.TryTakeOwner(out _).Should().BeFalse();
        message.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void TryTakeOwner_Owned_Rejects()
    {
        using var message = ZMessage.FromOwned([1, 2, 3]);
        message.TryTakeOwner(out _).Should().BeFalse();
    }

    [Fact]
    public void Dispose_IsIdempotent_AndAccessThrows()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        message.Dispose();
        message.Dispose();

        var act = () => message.GetFrame(0);
        act.Should().Throw<ObjectDisposedException>();
        var actFrameCount = () => message.FrameCount;
        actFrameCount.Should().Throw<ObjectDisposedException>();
        var actWhole = () => message.Whole;
        actWhole.Should().Throw<ObjectDisposedException>();
        var actTryContiguous = () => message.TryGetContiguousFrame(0, out _);
        actTryContiguous.Should().Throw<ObjectDisposedException>();
        var actTryOwner = () => message.TryTakeOwner(out _);
        actTryOwner.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void FrameSpanningSegments_IsSequence_NotContiguous()
    {
        using var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        byte[] expected = [1, 2, 3, 4, 5];

        message.TryGetContiguousFrame(0, out _).Should().BeFalse();
        message.GetFrame(0).ToArray().Should().Equal(expected);
        message.Whole.ToArray().Should().Equal(expected);
    }

    [Fact]
    public void GetFrame_OutOfRange_Throws()
    {
        using var message = ZMessage.FromOwned([1, 2, 3]);
        var act = () => message.GetFrame(1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
