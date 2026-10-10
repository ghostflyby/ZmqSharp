using System.Buffers;
using Xunit;

namespace ZmqSharp.Tests.Messages;

public sealed class ZMessageTests
{
    [Fact]
    public void FromOwned_IsSingleFrame_AndAliasesSource()
    {
        byte[] source = [1, 2, 3];
        var message = ZMessage.FromOwned(source);
        var frame = Assert.Single(message);
        Assert.Equal(source, frame.ToSequence().ToArray());
        Assert.True(message.TryGetValue(out ZSingleMessage single));
        var only = Assert.Single(single);
        Assert.True(only.TryGetValue(out ZSegment segment));
        Assert.Equal(source, segment.Memory.ToArray());

        source[1] = 9;
        Assert.Equal(9, segment.Memory.Span[1]);
        message.Dispose();
    }

    [Fact]
    public void Multipart_FramesAreAccessible()
    {
        var message = MessageFactory.Multipart([.. "ab"u8], [.. "cde"u8]);
        Assert.Equal(2, message.Count);
        Assert.True(message.TryGetValue(out ZMultiMessage multi));
        Assert.Equal(2, multi.Count);
        Assert.Equal([.. "ab"u8], message[0].ToSequence().ToArray());
        Assert.Equal([.. "cde"u8], message[1].ToSequence().ToArray());
        Assert.True(message[0].TryGetValue(out ZSegment first));
        Assert.Equal([.. "ab"u8], first.Memory.ToArray());
        message.Dispose();
    }

    [Fact]
    public void Enumerator_IsStruct_AndIteratesFrames()
    {
        var message = MessageFactory.Multipart([.. "a"u8], [.. "b"u8]);

        using var enumerator = message.GetEnumerator();
        Assert.True(enumerator.GetType().IsValueType);

        var frames = new List<byte[]>();
        foreach (var frame in message) frames.Add(frame.ToSequence().ToArray());

        Assert.Equal(2, frames.Count);
        Assert.Equal([.. "a"u8], frames[0]);
        Assert.Equal([.. "b"u8], frames[1]);
        message.Dispose();
    }

    [Fact]
    public void SegmentedFrame_IsNonContiguous_ButReadable()
    {
        var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        byte[] expected = [1, 2, 3, 4, 5];
        var frame = Assert.Single(message);
        Assert.False(frame.TryGetValue(out ZSegment _));
        Assert.True(frame.TryGetValue(out ZSegments segments));
        Assert.Equal(2, segments.Count);
        Assert.Equal(expected, frame.ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void SegmentedFrame_ManySegments_ReadsInOrder()
    {
        var message = MessageFactory.SegmentedFrame([1], [2], [3], [4], [5]);
        Assert.Equal([1, 2, 3, 4, 5], message[0].ToSequence().ToArray());
        Assert.Equal(5, message[0].ToSequence().Length);
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

        Assert.Equal(0, pool.Outstanding);
    }

    [Fact]
    public void Multi_Dispose_ReturnsAllPooledBuffers()
    {
        using var pool = new CountingMemoryPool();
        var firstOwner = pool.Rent(4);
        var secondOwner = pool.Rent(4);
        ZFrame[] frames =
        [
            new(new ZSegment(firstOwner, 0, 4)),
            new(new ZSegment(secondOwner, 0, 4))
        ];
        var multi = new ZMultiMessage(frames);

        multi.Dispose();
        multi.Dispose();

        Assert.Equal(0, pool.Outstanding);
    }

    [Fact]
    public void Indexer_OutOfRange_Throws()
    {
        var message = MessageFactory.Multipart([.. "a"u8], [.. "b"u8]);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = message[2]);
        message.Dispose();
    }

    [Fact]
    public void PooledMultipart_ReturnsBuffersOnDispose()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledMultipart(pool, [.. "a"u8], [.. "b"u8]);
        message.Dispose();
        Assert.Equal(0, pool.Outstanding);
    }

    [Fact]
    public void GetOwnedArray_ReturnsBackingArrayForOwnedSingleFrame()
    {
        byte[] source = [1, 2, 3];
        var message = ZMessage.FromOwned(source);
        Assert.True(message[0].TryGetValue(out ZSegment segment));

        Assert.True(segment.GetOwnedArray(out var array));
        Assert.Same(source, array);
        message.Dispose();
    }

    [Fact]
    public void Segment_SlicedOwned_RetainsOffsetView()
    {
        // An owned segment with a nonzero offset views a slice of the backing
        // array: content is the window, the owner is the same array (0006 3.4).
        byte[] source = [0, 1, 2, 3, 4, 5];
        var segment = new ZSegment(source, 2, 3);

        Assert.Equal([2, 3, 4], segment.Memory.ToArray());
        Assert.True(segment.GetOwnedArray(out var array));
        Assert.Same(source, array);

        // The view aliases the array: mutating the source is visible.
        source[2] = 9;
        Assert.Equal(9, segment.Memory.Span[0]);
    }

    [Fact]
    public void Segment_Empty_LengthZero()
    {
        var segment = new ZSegment(new byte[8], 4, 0);
        Assert.True(segment.Memory.IsEmpty);
    }

    [Fact]
    public void GetOwnedArray_FailsForPooledFrame()
    {
        using var pool = new CountingMemoryPool();
        var message = MessageFactory.PooledSingleFrame(pool, [.. "x"u8]);
        Assert.True(message[0].TryGetValue(out ZSegment segment));

        Assert.False(segment.GetOwnedArray(out _));
        message.Dispose();
        Assert.Equal(0, pool.Outstanding);
    }

    [Fact]
    public void GetOwnedArray_FailsForSegmentedFrame()
    {
        var message = MessageFactory.SegmentedFrame([1], [2]);
        Assert.False(message[0].TryGetValue(out ZSegment _));
        message.Dispose();
    }

    [Fact]
    public void Frame_SingleSegment_IsReadOnlyListOfOne()
    {
        var message = MessageFactory.SingleFrame([1, 2, 3]);
        var frame = message[0];

        var first = Assert.Single(frame);
        Assert.Equal([1, 2, 3], first.Memory.ToArray());

        var segments = new List<byte[]>();
        foreach (var segment in frame) segments.Add(segment.Memory.ToArray());

        var item = Assert.Single(segments);
        Assert.Equal([1, 2, 3], item);
        message.Dispose();
    }

    [Fact]
    public void Frame_Segmented_IsReadOnlyListOfSegments()
    {
        var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5]);
        var frame = message[0];

        Assert.Equal(2, frame.Count);
        Assert.Equal([1, 2, 3], frame[0].Memory.ToArray());
        Assert.Equal([4, 5], frame[1].Memory.ToArray());

        var segments = new List<byte[]>();
        foreach (var segment in frame) segments.Add(segment.Memory.ToArray());

        Assert.Equal(2, segments.Count);
        Assert.Equal([1, 2, 3], segments[0]);
        Assert.Equal([4, 5], segments[1]);
        message.Dispose();
    }

    [Fact]
    public void Segment_IsReadOnlyListOfItself()
    {
        var message = MessageFactory.SingleFrame([1, 2, 3]);
        var segment = message[0][0];

        var first = Assert.Single(segment);
        Assert.Equal([1, 2, 3], first.Memory.ToArray());

        var items = new List<byte[]>();
        foreach (var item in segment) items.Add(item.Memory.ToArray());

        var only = Assert.Single(items);
        Assert.Equal([1, 2, 3], only);
        message.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SegmentToFrame()
    {
        byte[] data = [1, 2, 3];
        ZFrame frame = new ZSegment(data, 0, data.Length);

        Assert.True(frame.TryGetValue(out ZSegment segment));
        Assert.Equal(data, segment.Memory.ToArray());
        Assert.False(frame.TryGetValue(out ZSegments _));
        frame.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SegmentsToFrame()
    {
        var message = MessageFactory.SegmentedFrame([1, 2], [3, 4, 5]);
        Assert.True(message[0].TryGetValue(out ZSegments segments));
        ZFrame frame = segments;

        Assert.True(frame.TryGetValue(out ZSegments converted));
        Assert.Equal(2, converted.Count);
        Assert.Equal([1, 2, 3, 4, 5], frame.ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void ImplicitConversion_SingleToMessage()
    {
        var message = ZMessage.FromOwned([1, 2, 3]);
        Assert.True(message.TryGetValue(out ZSingleMessage single));
        ZMessage converted = single;

        Assert.True(converted.TryGetValue(out ZSingleMessage convertedSingle));
        Assert.Single(convertedSingle);
        Assert.Equal([1, 2, 3], converted[0].ToSequence().ToArray());
        converted.Dispose();
    }

    [Fact]
    public void PerCaseEnumerators_IterateAndGuard()
    {
        // The case enumerators (ZSingleMessage.Enumerator / ZMultiMessage.Enumerator)
        // are the structs behind the wrapper; exercise them directly.
        var single = MessageFactory.SingleFrame([1, 2, 3]);
        Assert.True(single.TryGetValue(out ZSingleMessage singleMsg));
        using var singleEnum = singleMsg.GetEnumerator();
        Assert.Throws<InvalidOperationException>(() => _ = singleEnum.Current);
        Assert.True(singleEnum.MoveNext());
        Assert.Equal([1, 2, 3], singleEnum.Current.ToSequence().ToArray());
        Assert.False(singleEnum.MoveNext());
        Assert.Throws<InvalidOperationException>(() => _ = singleEnum.Current);
        singleEnum.Reset();
        Assert.True(singleEnum.MoveNext());
        single.Dispose();

        var multi = MessageFactory.Multipart([.. "a"u8], [.. "b"u8]);
        Assert.True(multi.TryGetValue(out ZMultiMessage multiMsg));
        using var multiEnum = multiMsg.GetEnumerator();
        Assert.Throws<InvalidOperationException>(() => _ = multiEnum.Current);
        var frames = new List<byte[]>();
        while (multiEnum.MoveNext()) frames.Add(multiEnum.Current.ToSequence().ToArray());

        Assert.Equal(2, frames.Count);
        Assert.Throws<InvalidOperationException>(() => _ = multiEnum.Current);
        multiEnum.Reset();
        Assert.True(multiEnum.MoveNext());

        // IReadOnlyList<T>.GetEnumerator routes to the case enumerators.
        var explicitSingle = ((IEnumerable<ZFrame>)singleMsg).GetEnumerator();
        Assert.True(explicitSingle.MoveNext());
        explicitSingle.Dispose();
        var explicitMulti = ((IEnumerable<ZFrame>)multiMsg).GetEnumerator();
        Assert.True(explicitMulti.MoveNext());
        explicitMulti.Dispose();

        single.Dispose();
        multi.Dispose();
    }

    [Fact]
    public void Enumerators_ResetAndBoundaryBehavior()
    {
        // Single-message enumerator: exactly one element, then exhaustion;
        // Current before start or after end throws.
        var single = MessageFactory.SingleFrame([1, 2, 3]);
        using var singleEnum = single.GetEnumerator();
        Assert.Throws<InvalidOperationException>(() => _ = singleEnum.Current);
        Assert.True(singleEnum.MoveNext());
        Assert.Equal([1, 2, 3], singleEnum.Current.ToSequence().ToArray());
        Assert.False(singleEnum.MoveNext());
        Assert.Throws<InvalidOperationException>(() => _ = singleEnum.Current);
        singleEnum.Reset();
        Assert.True(singleEnum.MoveNext());
        single.Dispose();

        // Multi-message enumerator: Reset restarts iteration.
        var multi = MessageFactory.Multipart([.. "a"u8], [.. "b"u8]);
        using var multiEnum = multi.GetEnumerator();
        Assert.True(multiEnum.MoveNext());
        Assert.True(multiEnum.MoveNext());
        Assert.False(multiEnum.MoveNext());
        multiEnum.Reset();
        var frames = new List<byte[]>();
        while (multiEnum.MoveNext()) frames.Add(multiEnum.Current.ToSequence().ToArray());

        Assert.Equal(2, frames.Count);
        Assert.Equal([.. "a"u8], frames[0]);
        Assert.Equal([.. "b"u8], frames[1]);
        multi.Dispose();

        // Index out of range throws on both cases.
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = single[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = multi[2]);
    }

    [Fact]
    public void ImplicitConversion_MultiToMessage()
    {
        var message = MessageFactory.Multipart([.. "ab"u8], [.. "cde"u8]);
        Assert.True(message.TryGetValue(out ZMultiMessage multi));
        ZMessage converted = multi;

        Assert.True(converted.TryGetValue(out ZMultiMessage convertedMulti));
        Assert.Equal(2, convertedMulti.Count);
        Assert.Equal([.. "ab"u8], converted[0].ToSequence().ToArray());
        Assert.Equal([.. "cde"u8], converted[1].ToSequence().ToArray());
        converted.Dispose();
    }
}
