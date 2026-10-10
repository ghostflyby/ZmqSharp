using System.Buffers;
using Xunit;

namespace ZmqSharp.Tests.Messages;

/// <summary>
/// Public message construction (0026): the copy face (<c>Copy</c>) and the
/// ownership-transfer face (<c>FromOwned</c> / <c>FromPooled</c>), and how
/// each maps onto the single/multipart × contiguous/non-contiguous cases.
/// </summary>
public sealed class ZMessageConstructionTests
{
    [Fact]
    public void Copy_ReadOnlyMemory_SingleFrameOwned()
    {
        var source = "hello"u8.ToArray();

        var message = ZMessage.Copy(source);

        Assert.True(message.TryGetValue(out ZSingleMessage single));
        Assert.Equal(source, single[0].ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void Copy_Enumerable_MultipartOwnedFramePerElement()
    {
        ReadOnlyMemory<byte>[] frames = ["a"u8.ToArray(), "bb"u8.ToArray(), "ccc"u8.ToArray()];

        var message = ZMessage.Copy(frames);

        Assert.True(message.TryGetValue(out ZMultiMessage _));
        Assert.Equal(3, message.Count);
        Assert.Equal((byte[])[.. "a"u8], message[0].ToSequence().ToArray());
        Assert.Equal((byte[])[.. "bb"u8], message[1].ToSequence().ToArray());
        Assert.Equal((byte[])[.. "ccc"u8], message[2].ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void Copy_Enumerable_SingleElementIsOneFrameMessage()
    {
        var message = ZMessage.Copy(new[] { new ReadOnlyMemory<byte>("x"u8.ToArray()) });

        Assert.Single(message);
        Assert.Equal((byte[])[.. "x"u8], message[0].ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void Copy_SingleSegmentSequence_CollapsesToContiguous()
    {
        var message = ZMessage.Copy(new ReadOnlySequence<byte>("payload"u8.ToArray()));

        Assert.True(message.TryGetValue(out ZSingleMessage single));
        Assert.True(single[0].TryGetValue(out ZSegment _));
        Assert.Equal((byte[])[.. "payload"u8], single[0].ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void Copy_MultiSegmentSequence_YieldsNonContiguousFrame()
    {
        var first = "abc"u8.ToArray();
        var second = "def"u8.ToArray();
        var third = "ghi"u8.ToArray();
        var seg1 = new BufferSegment(first, 0);
        var seg2 = new BufferSegment(second, first.Length);
        var seg3 = new BufferSegment(third, first.Length + second.Length);
        seg1.Link = seg2;
        seg2.Link = seg3;
        var sequence = new ReadOnlySequence<byte>(seg1, 0, seg3, seg3.Memory.Length);

        var message = ZMessage.Copy(sequence);

        Assert.True(message.TryGetValue(out ZSingleMessage single));
        Assert.True(single[0].TryGetValue(out ZSegments segments));
        Assert.Equal(3, segments.Count);
        Assert.Equal(first, segments[0].Memory.ToArray());
        Assert.Equal(second, segments[1].Memory.ToArray());
        Assert.Equal(third, segments[2].Memory.ToArray());
        message.Dispose();
    }

    [Fact]
    public void FromOwned_MultiFrame_ZeroCopyOwnedFrames()
    {
        var frames = new[] { "a"u8.ToArray(), "b"u8.ToArray() };

        var message = ZMessage.FromOwned(frames);

        Assert.True(message.TryGetValue(out ZMultiMessage multi));
        Assert.Equal(2, multi.Count);
        Assert.Equal(frames[0], message[0].ToSequence().ToArray());
        Assert.Equal(frames[1], message[1].ToSequence().ToArray());
        message.Dispose();
    }

    [Fact]
    public void FromPooled_SingleFrame_OwnsPooledBuffer()
    {
        var owner = MemoryPool<byte>.Shared.Rent(4);
        "data"u8.CopyTo(owner.Memory.Span);

        var message = ZMessage.FromPooled(owner);

        Assert.True(message.TryGetValue(out ZSingleMessage single));
        // The pool grants at least the requested size; the message covers the
        // whole rented segment (the ownership-transfer contract).
        Assert.Equal((byte[])[.. "data"u8], single[0].ToSequence().ToArray()[..4]);
        message.Dispose();
    }

    [Fact]
    public void FromOwned_EmptyFrameArray_Throws()
    {
        Assert.Throws<ArgumentException>(() => ZMessage.FromOwned(Array.Empty<byte[]>()));
    }

    [Fact]
    public void Copy_EmptyEnumerable_Throws()
    {
        Assert.Throws<ArgumentException>(() => ZMessage.Copy(Array.Empty<ReadOnlyMemory<byte>>()));
    }

    [Fact]
    public void FromOwned_SingleFrameArray_ZeroCopy()
    {
        var data = "owned"u8.ToArray();

        var message = ZMessage.FromOwned(data);

        Assert.True(message.TryGetValue(out ZSingleMessage single));
        Assert.Equal(data, single[0].ToSequence().ToArray());
        message.Dispose();
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(byte[] memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public ReadOnlySequenceSegment<byte>? Link
        {
            set => Next = value;
        }
    }
}
