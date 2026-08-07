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
        message.Count.Should().Be(1);
        message[0].ToArray().Should().Equal(source);
        message.TryGetContiguousFrame(0, out var memory).Should().BeTrue();
        memory.ToArray().Should().Equal(source);

        source[1] = 9;
        message[0].FirstSpan[1].Should().Be(9);
    }

    [Fact]
    public void Multipart_FramesAreAccessible()
    {
        using var message = MessageFactory.Multipart("ab"u8.ToArray(), "cde"u8.ToArray());
        message.Count.Should().Be(2);
        message[0].ToArray().Should().Equal("ab"u8.ToArray());
        message[1].ToArray().Should().Equal("cde"u8.ToArray());
        message.TryGetContiguousFrame(1, out var memory).Should().BeTrue();
        memory.ToArray().Should().Equal("cde"u8.ToArray());

        var enumerated = message.Select(frame => frame.ToArray()).ToArray();
        enumerated.Should().HaveCount(2);
        enumerated[0].Should().Equal("ab"u8.ToArray());
        enumerated[1].Should().Equal("cde"u8.ToArray());
    }

    [Fact]
    public void Enumerator_IsStruct_AndIteratesFrames()
    {
        using var message = MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray());

        message.GetEnumerator().GetType().IsValueType.Should().BeTrue();

        var frames = new List<byte[]>();
        foreach (var frame in message)
        {
            frames.Add(frame.ToArray());
        }

        frames.Should().HaveCount(2);
        frames[0].Should().Equal("a"u8.ToArray());
        frames[1].Should().Equal("b"u8.ToArray());
    }

    [Fact]
    public void SegmentedFrame_IsNotContiguous_ButReadable()
    {
        using var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        byte[] expected = [1, 2, 3, 4, 5];
        message.Count.Should().Be(1);
        message.TryGetContiguousFrame(0, out _).Should().BeFalse();
        message[0].ToArray().Should().Equal(expected);

        message.GetEnumerator().GetType().IsValueType.Should().BeTrue();
        message.Should().ContainSingle().Which.ToArray().Should().Equal(expected);
    }

    [Fact]
    public void SegmentedFrame_ManySegments_ReadsInOrder()
    {
        using var message = MessageFactory.SegmentedFrame([1], [2], [3], [4], [5]);

        message[0].ToArray().Should().Equal([1, 2, 3, 4, 5]);
        message[0].Length.Should().Be(5);
    }

    [Fact]
    public void Dispose_IsIdempotent_AndAccessThrows()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        message.Dispose();
        message.Dispose();

        var act = () => message[0];
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Indexer_OutOfRange_Throws()
    {
        using var message = MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray());
        var act = () => message[2];
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
