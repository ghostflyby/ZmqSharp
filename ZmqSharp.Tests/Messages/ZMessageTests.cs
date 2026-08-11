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
        var message = ZMessage.FromOwned(source);
        message.Count.Should().Be(1);
        message[0].ToSequence().ToArray().Should().Equal(source);
        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        single.Count.Should().Be(1);
        single[0].TryGetValue(out ZSegment segment).Should().BeTrue();
        segment.Memory.ToArray().Should().Equal(source);

        source[1] = 9;
        segment.Memory.Span[1].Should().Be(9);
        message.Dispose();
    }

    [Fact]
    public void Multipart_FramesAreAccessible()
    {
        var message = MessageFactory.Multipart("ab"u8.ToArray(), "cde"u8.ToArray());
        message.Count.Should().Be(2);
        message.TryGetValue(out ZMultiMessage multi).Should().BeTrue();
        multi.Count.Should().Be(2);
        message[0].ToSequence().ToArray().Should().Equal("ab"u8.ToArray());
        message[1].ToSequence().ToArray().Should().Equal("cde"u8.ToArray());
        message[0].TryGetValue(out ZSegment first).Should().BeTrue();
        first.Memory.ToArray().Should().Equal("ab"u8.ToArray());
        message.Dispose();
    }

    [Fact]
    public void Enumerator_IsStruct_AndIteratesFrames()
    {
        var message = MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray());

        var enumerator = message.GetEnumerator();
        enumerator.GetType().IsValueType.Should().BeTrue();

        var frames = new List<byte[]>();
        foreach (var frame in message)
        {
            frames.Add(frame.ToSequence().ToArray());
        }

        frames.Should().HaveCount(2);
        frames[0].Should().Equal("a"u8.ToArray());
        frames[1].Should().Equal("b"u8.ToArray());
        message.Dispose();
    }

    [Fact]
    public void SegmentedFrame_IsNonContiguous_ButReadable()
    {
        var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        byte[] expected = [1, 2, 3, 4, 5];
        message.Count.Should().Be(1);
        message[0].TryGetValue(out ZSegment _).Should().BeFalse();
        message[0].TryGetValue(out ZSegments segments).Should().BeTrue();
        segments.Count.Should().Be(2);
        message[0].ToSequence().ToArray().Should().Equal(expected);
        message.Dispose();
    }

    [Fact]
    public void SegmentedFrame_ManySegments_ReadsInOrder()
    {
        var message = MessageFactory.SegmentedFrame([1], [2], [3], [4], [5]);
        message[0].ToSequence().ToArray().Should().Equal([1, 2, 3, 4, 5]);
        message[0].ToSequence().Length.Should().Be(5);
        message.Dispose();
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        message.Dispose();
        message.Dispose();
    }

    [Fact]
    public void Single_Dispose_ReturnsPooledBuffer()
    {
        using var pool = new CountingMemoryPool();
        var owner = pool.Rent(4);
        var single = new ZSingleMessage(new ZFrame(new ZSegment(owner, 0, 4)));

        single.Dispose();
        single.Dispose();

        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void Multi_Dispose_ReturnsAllPooledBuffers()
    {
        using var pool = new CountingMemoryPool();
        var firstOwner = pool.Rent(4);
        var secondOwner = pool.Rent(4);
        ZFrame[] frames = [
            new ZFrame(new ZSegment(firstOwner, 0, 4)),
            new ZFrame(new ZSegment(secondOwner, 0, 4)),
        ];
        var multi = new ZMultiMessage(frames);

        multi.Dispose();
        multi.Dispose();

        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void Indexer_OutOfRange_Throws()
    {
        var message = MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray());
        message.Invoking(act => act[2]).Should()
            .Throw<ArgumentOutOfRangeException>();
        message.Dispose();
    }

    [Fact]
    public void PooledMultipart_ReturnsBuffersOnDispose()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledMultipart(pool, "a"u8.ToArray(), "b"u8.ToArray());
        message.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void GetOwnedArray_ReturnsBackingArrayForOwnedSingleFrame()
    {
        byte[] source = [1, 2, 3];
        var message = ZMessage.FromOwned(source);
        message[0].TryGetValue(out ZSegment segment).Should().BeTrue();

        segment.GetOwnedArray(out var array).Should().BeTrue();
        array.Should().BeSameAs(source);
        message.Dispose();
    }

    [Fact]
    public void Segment_SlicedOwned_RetainsOffsetView()
    {
        // An owned segment with a nonzero offset views a slice of the backing
        // array: content is the window, the owner is the same array (0006 3.4).
        byte[] source = [0, 1, 2, 3, 4, 5];
        var segment = new ZSegment(source, 2, 3);

        segment.Memory.ToArray().Should().Equal([2, 3, 4]);
        segment.GetOwnedArray(out var array).Should().BeTrue();
        array.Should().BeSameAs(source);

        // The view aliases the array: mutating the source is visible.
        source[2] = 9;
        segment.Memory.Span[0].Should().Be(9);
    }

    [Fact]
    public void Segment_Empty_LengthZero()
    {
        var segment = new ZSegment(new byte[8], 4, 0);
        segment.Memory.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void GetOwnedArray_FailsForPooledFrame()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledSingleFrame(pool, "x"u8.ToArray());
        message[0].TryGetValue(out ZSegment segment).Should().BeTrue();

        segment.GetOwnedArray(out _).Should().BeFalse();
        message.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public void GetOwnedArray_FailsForSegmentedFrame()
    {
        var message = MessageFactory.SegmentedFrame([1], [2]);
        message[0].TryGetValue(out ZSegment _).Should().BeFalse();
        message.Dispose();
    }

    [Fact]
    public void Frame_SingleSegment_IsReadOnlyListOfOne()
    {
        var message = MessageFactory.SingleFrame([1, 2, 3]);
        var frame = message[0];

        frame.Count.Should().Be(1);
        frame[0].Memory.ToArray().Should().Equal([1, 2, 3]);

        var segments = new List<byte[]>();
        foreach (var segment in frame)
        {
            segments.Add(segment.Memory.ToArray());
        }

        segments.Should().HaveCount(1);
        segments[0].Should().Equal([1, 2, 3]);
        message.Dispose();
    }

    [Fact]
    public void Frame_Segmented_IsReadOnlyListOfSegments()
    {
        var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        var frame = message[0];

        frame.Count.Should().Be(2);
        frame[0].Memory.ToArray().Should().Equal([1, 2, 3]);
        frame[1].Memory.ToArray().Should().Equal([4, 5]);

        var segments = new List<byte[]>();
        foreach (var segment in frame)
        {
            segments.Add(segment.Memory.ToArray());
        }

        segments.Should().HaveCount(2);
        segments[0].Should().Equal([1, 2, 3]);
        segments[1].Should().Equal([4, 5]);
        message.Dispose();
    }

    [Fact]
    public void Segment_IsReadOnlyListOfItself()
    {
        var message = MessageFactory.SingleFrame([1, 2, 3]);
        var segment = message[0][0];

        segment.Count.Should().Be(1);
        segment[0].Memory.ToArray().Should().Equal([1, 2, 3]);

        var items = new List<byte[]>();
        foreach (var item in segment)
        {
            items.Add(item.Memory.ToArray());
        }

        items.Should().HaveCount(1);
        items[0].Should().Equal([1, 2, 3]);
        message.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SegmentToFrame()
    {
        byte[] data = [1, 2, 3];
        ZFrame frame = new ZSegment(data, 0, data.Length);

        frame.TryGetValue(out ZSegment segment).Should().BeTrue();
        segment.Memory.ToArray().Should().Equal(data);
        frame.TryGetValue(out ZSegments _).Should().BeFalse();
        frame.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SegmentsToFrame()
    {
        var message = MessageFactory.SegmentedFrame([1, 2], [3, 4, 5]);
        message[0].TryGetValue(out ZSegments segments).Should().BeTrue();
        ZFrame frame = segments;

        frame.TryGetValue(out ZSegments converted).Should().BeTrue();
        converted.Count.Should().Be(2);
        frame.ToSequence().ToArray().Should().Equal([1, 2, 3, 4, 5]);
        message.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SingleToMessage()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        ZMessage converted = single;

        converted.TryGetValue(out ZSingleMessage convertedSingle).Should().BeTrue();
        convertedSingle.Count.Should().Be(1);
        converted[0].ToSequence().ToArray().Should().Equal([1, 2, 3]);
        converted.Dispose();
    }

    [Fact]
    public void ImplicitConversion_MultiToMessage()
    {
        var message = MessageFactory.Multipart("ab"u8.ToArray(), "cde"u8.ToArray());
        message.TryGetValue(out ZMultiMessage multi).Should().BeTrue();
        ZMessage converted = multi;

        converted.TryGetValue(out ZMultiMessage convertedMulti).Should().BeTrue();
        convertedMulti.Count.Should().Be(2);
        converted[0].ToSequence().ToArray().Should().Equal("ab"u8.ToArray());
        converted[1].ToSequence().ToArray().Should().Equal("cde"u8.ToArray());
        converted.Dispose();
    }
}
