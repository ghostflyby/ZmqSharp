using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests;

/// <summary>
/// Pins the receive full-mode mapping and the BCL bounded-channel drop
/// contract the socket relies on (0006 section 3.5: the item-dropped callback
/// receives the item selected by each mode).
/// </summary>
public sealed class ZQueueFullModeTests
{
    [Theory]
    [InlineData(ZQueueFullMode.Wait, BoundedChannelFullMode.Wait)]
    [InlineData(ZQueueFullMode.DropWrite, BoundedChannelFullMode.DropWrite)]
    [InlineData(ZQueueFullMode.DropNewest, BoundedChannelFullMode.DropNewest)]
    [InlineData(ZQueueFullMode.DropOldest, BoundedChannelFullMode.DropOldest)]
    public void FullMode_MapsToBoundedChannelFullMode(ZQueueFullMode mode, BoundedChannelFullMode expected)
        => ZBoundedQueueFactory.ToBoundedFullMode(mode).Should().Be(expected);

    [Fact]
    public void FullMode_InvalidValue_Throws()
    {
        var act = () => ZBoundedQueueFactory.ToBoundedFullMode((ZQueueFullMode)99);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void DropWrite_ItemDropped_ReceivesIncomingItem()
    {
        var dropped = new List<int>();
        var channel = Channel.CreateBounded<int>(
            new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropWrite },
            dropped.Add);

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3).Should().BeTrue();

        dropped.Should().Equal([3]);
    }

    [Fact]
    public void DropNewest_ItemDropped_ReceivesNewestBufferedItem()
    {
        var dropped = new List<int>();
        var channel = Channel.CreateBounded<int>(
            new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropNewest },
            dropped.Add);

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3).Should().BeTrue();

        dropped.Should().Equal([2]);
        channel.Reader.TryRead(out var first).Should().BeTrue();
        first.Should().Be(1);
        channel.Reader.TryRead(out var second).Should().BeTrue();
        second.Should().Be(3);
    }

    [Fact]
    public void DropOldest_ItemDropped_ReceivesOldestBufferedItem()
    {
        var dropped = new List<int>();
        var channel = Channel.CreateBounded<int>(
            new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropOldest },
            dropped.Add);

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3).Should().BeTrue();

        dropped.Should().Equal([1]);
        channel.Reader.TryRead(out var first).Should().BeTrue();
        first.Should().Be(2);
        channel.Reader.TryRead(out var second).Should().BeTrue();
        second.Should().Be(3);
    }
}
