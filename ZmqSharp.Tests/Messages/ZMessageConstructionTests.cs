using System.Buffers;
using FluentAssertions;
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

        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        single[0].ToSequence().ToArray().Should().Equal(source);
        message.Dispose();
    }

    [Fact]
    public void Copy_Enumerable_MultipartOwnedFramePerElement()
    {
        ReadOnlyMemory<byte>[] frames = ["a"u8.ToArray(), "bb"u8.ToArray(), "ccc"u8.ToArray()];

        var message = ZMessage.Copy(frames);

        message.TryGetValue(out ZMultiMessage multi).Should().BeTrue();
        message.Count.Should().Be(3);
        message[0].ToSequence().ToArray().Should().Equal([.. "a"u8]);
        message[1].ToSequence().ToArray().Should().Equal([.. "bb"u8]);
        message[2].ToSequence().ToArray().Should().Equal([.. "ccc"u8]);
        message.Dispose();
    }

    [Fact]
    public void Copy_Enumerable_SingleElementIsOneFrameMessage()
    {
        var message = ZMessage.Copy(new[] { new ReadOnlyMemory<byte>("x"u8.ToArray()) });

        message.Count.Should().Be(1);
        message[0].ToSequence().ToArray().Should().Equal([.. "x"u8]);
        message.Dispose();
    }

    [Fact]
    public void Copy_SingleSegmentSequence_CollapsesToContiguous()
    {
        var message = ZMessage.Copy(new ReadOnlySequence<byte>("payload"u8.ToArray()));

        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        single[0].TryGetValue(out ZSegment segment).Should().BeTrue();
        single[0].ToSequence().ToArray().Should().Equal([.. "payload"u8]);
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

        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        single[0].TryGetValue(out ZSegments segments).Should().BeTrue();
        segments.Count.Should().Be(3);
        segments[0].Memory.ToArray().Should().Equal(first);
        segments[1].Memory.ToArray().Should().Equal(second);
        segments[2].Memory.ToArray().Should().Equal(third);
        message.Dispose();
    }

    [Fact]
    public void FromOwned_MultiFrame_ZeroCopyOwnedFrames()
    {
        var frames = new[] { "a"u8.ToArray(), "b"u8.ToArray() };

        var message = ZMessage.FromOwned(frames);

        message.TryGetValue(out ZMultiMessage multi).Should().BeTrue();
        multi.Count.Should().Be(2);
        message[0].ToSequence().ToArray().Should().Equal(frames[0]);
        message[1].ToSequence().ToArray().Should().Equal(frames[1]);
        message.Dispose();
    }

    [Fact]
    public void FromPooled_SingleFrame_OwnsPooledBuffer()
    {
        var owner = MemoryPool<byte>.Shared.Rent(4);
        "data"u8.CopyTo(owner.Memory.Span);

        var message = ZMessage.FromPooled(owner);

        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        // The pool grants at least the requested size; the message covers the
        // whole rented segment (the ownership-transfer contract).
        single[0].ToSequence().ToArray()[..4].Should().Equal([.. "data"u8]);
        message.Dispose();
    }

    [Fact]
    public void FromOwned_EmptyFrameArray_Throws()
    {
        var act = () => ZMessage.FromOwned(Array.Empty<byte[]>());
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Copy_EmptyEnumerable_Throws()
    {
        var act = () => ZMessage.Copy(Array.Empty<ReadOnlyMemory<byte>>());
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void FromOwned_SingleFrameArray_ZeroCopy()
    {
        var data = "owned"u8.ToArray();

        var message = ZMessage.FromOwned(data);

        message.TryGetValue(out ZSingleMessage single).Should().BeTrue();
        single[0].ToSequence().ToArray().Should().Equal(data);
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
