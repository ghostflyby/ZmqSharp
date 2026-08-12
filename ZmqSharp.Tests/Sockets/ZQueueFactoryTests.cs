using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
///     Pins the 0009 queue-factory contract: every public use goes
///     through a BCL options instance converted implicitly into a factory (the
///     concrete factories are internal); the factory forces SingleReader,
///     preserves SingleWriter, copies at construction, and wires itemDropped.
/// </summary>
public sealed class ZQueueFactoryTests
{
    private static ZMessage Item(byte value = 1)
    {
        return MessageFactory.SingleFrame([value]);
    }

    [Fact]
    public void Factory_ForcesSingleReader_AndPreservesSingleWriter()
    {
        // SingleReader/SingleWriter are caller promises in BCL channels (a
        // violation is undefined behavior, not an exception), so the effective
        // flags are asserted on the factory's fixed snapshot instead.
        var factory = new ZBoundedQueueFactory(new BoundedChannelOptions(4) { SingleWriter = false });
        factory.Options.SingleReader.Should().BeTrue();
        factory.Options.SingleWriter.Should().BeFalse();
    }

    [Fact]
    public void Factory_SingleWriterTrue_IsPreserved()
    {
        var factory = new ZBoundedQueueFactory(new BoundedChannelOptions(4) { SingleWriter = true });
        factory.Options.SingleReader.Should().BeTrue();
        factory.Options.SingleWriter.Should().BeTrue();
    }

    [Fact]
    public async Task Factory_SingleWriterFalse_ConcurrentWritesWork()
    {
        // The outbound channel is a shared producer surface: singleWriter must
        // be false there, and the factory preserves the caller's choice.
        var channel = new ZBoundedQueueFactory(new BoundedChannelOptions(4) { SingleWriter = false }).Create(_ => { });
        var accepted = 0;
        var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            if (channel.Writer.TryWrite(Item())) Interlocked.Increment(ref accepted);
        })).ToArray();
        await Task.WhenAll(writers);

        accepted.Should().Be(4);
        Drain(channel);
    }

    [Fact]
    public void Factory_ReusesBclOptions_CopiesAtConstruction()
    {
        // The caller's options instance is copied at construction; mutating it
        // later must not change the factory's snapshot.
        var options = new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleWriter = false
        };
        var factory = new ZBoundedQueueFactory(options);

        options.FullMode = BoundedChannelFullMode.DropOldest;

        var channel = factory.Create(_ => { });
        channel.Writer.TryWrite(Item());
        channel.Writer.TryWrite(Item(2));
        channel.Writer.TryWrite(Item(3)).Should().BeTrue();
        channel.Reader.Count.Should().Be(1);
        channel.Reader.TryRead(out var kept).Should().BeTrue();
        kept[0].ToSequence().ToArray().Should().Equal(1);
        kept.Dispose();
    }

    [Fact]
    public void Factory_WiresItemDropped()
    {
        var factory = new ZBoundedQueueFactory(new BoundedChannelOptions(1)
        { FullMode = BoundedChannelFullMode.DropWrite });
        var dropped = new List<ZMessage>();
        var channel = factory.Create(dropped.Add);

        channel.Writer.TryWrite(Item()).Should().BeTrue();
        channel.Writer.TryWrite(Item(2)).Should().BeTrue();

        dropped.Should().ContainSingle();
        dropped[0][0].ToSequence().ToArray().Should().Equal(2);
        dropped[0].Dispose();
    }

    [Fact]
    public void UnboundedFactory_IgnoresItemDropped_AndNeverDrops()
    {
        var factory = new ZUnboundedQueueFactory(new UnboundedChannelOptions());
        var dropped = new List<ZMessage>();
        var channel = factory.Create(dropped.Add);

        for (var i = 0; i < 100; i++) channel.Writer.TryWrite(Item((byte)i)).Should().BeTrue();

        dropped.Should().BeEmpty();
        Drain(channel);
        channel.Reader.Completion.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public void ImplicitConversion_FromBoundedOptions()
    {
        ZQueueFactory factory = new BoundedChannelOptions(16);
        factory.Should().BeOfType<ZBoundedQueueFactory>();
        var channel = factory.Create(_ => { });
        channel.Reader.Count.Should().Be(0);
    }

    [Fact]
    public void ImplicitConversion_FromUnboundedOptions()
    {
        ZQueueFactory factory = new UnboundedChannelOptions();
        factory.Should().BeOfType<ZUnboundedQueueFactory>();
        var channel = factory.Create(_ => { });
        channel.Writer.TryWrite(Item()).Should().BeTrue();
        Drain(channel);
    }

    [Fact]
    public void Factory_InvalidCapacity_Throws()
    {
        var act = () => new BoundedChannelOptions(-1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Factory_InvalidFullMode_Throws()
    {
        // BoundedChannelOptions validates the full mode in its setter, so the
        // failure surfaces at the options construction the factory consumes.
        var act = () => new BoundedChannelOptions(4) { FullMode = (BoundedChannelFullMode)99 };
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void StrategyContract_IsSatisfiedByBaseAndConcreteFactories()
    {
        // The concrete factories are internal (0009 D1): every public use goes
        // through the ZQueueFactory base and its implicit conversions, and the
        // base satisfies the IZQueueFactory strategy contract.
        ZQueueFactory fromOptions = new BoundedChannelOptions(16);
        IZQueueFactory viaBase = new ZBoundedQueueFactory(new BoundedChannelOptions(16));
        fromOptions.Should().BeOfType<ZBoundedQueueFactory>();
        viaBase.Create(_ => { });
        viaBase.Should().BeOfType<ZBoundedQueueFactory>();
    }

    [Fact]
    public void Options_Defaults_ReceiveBoundedSendDisabled()
    {
        var options = new ZQueueSocketOptions();
        options.ReceiveQueueFactory.Should().BeOfType<ZBoundedQueueFactory>();
        options.SendQueueFactory.Should().BeNull();
    }

    private static void Drain(Channel<ZMessage> channel)
    {
        while (channel.Reader.TryRead(out var item)) item.Dispose();
    }
}
