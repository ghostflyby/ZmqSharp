using System.Threading.Channels;
using Xunit;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
///     Pins the BCL bounded-channel drop contract the socket relies on (0006
///     section 3.5: the item-dropped callback receives the item selected by each
///     drop mode), exercised through the options the factory applies.
/// </summary>
public sealed class BoundedChannelDropTests
{
    [Fact]
    public void DropWrite_ItemDropped_ReceivesIncomingItem()
    {
        var dropped = new List<int>();
        var channel = Channel.CreateBounded<int>(
            new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropWrite },
            dropped.Add);

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        Assert.True(channel.Writer.TryWrite(3));

        Assert.Equal([3], dropped);
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
        Assert.True(channel.Writer.TryWrite(3));

        Assert.Equal([2], dropped);
        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal(1, first);
        Assert.True(channel.Reader.TryRead(out var second));
        Assert.Equal(3, second);
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
        Assert.True(channel.Writer.TryWrite(3));

        Assert.Equal([1], dropped);
        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal(2, first);
        Assert.True(channel.Reader.TryRead(out var second));
        Assert.Equal(3, second);
    }
}
