using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

public sealed class ZmtpParserTests
{
    private static byte[] Payload(int length) => Enumerable.Range(0, length).Select(i => (byte)(i % 251)).ToArray();

    [Fact]
    public async Task SingleFrame_Pooled_IsDelivered()
    {
        var payload = "hello"u8.ToArray();
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.FrameCount.Should().Be(1);
        message.GetOrigin(0).Should().Be(ZBufferOrigin.Pooled);
        message.GetFrame(0).ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task Multipart_IsAssembledAtLastFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("A"u8.ToArray(), more: true),
            ZmtpTestData.Frame("B"u8.ToArray(), more: true),
            ZmtpTestData.Frame("C"u8.ToArray())));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.FrameCount.Should().Be(3);
        message.GetFrame(0).ToArray().Should().Equal("A"u8.ToArray());
        message.GetFrame(1).ToArray().Should().Equal("B"u8.ToArray());
        message.GetFrame(2).ToArray().Should().Equal("C"u8.ToArray());
        message.Whole.ToArray().Should().Equal("ABC"u8.ToArray());
        message.Dispose();
    }

    [Fact]
    public async Task SplitReads_ByteByByte_StillParses()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("A"u8.ToArray(), more: true),
            ZmtpTestData.Frame("B"u8.ToArray()));
        var source = new ChunkedMemoryStream(wire, maxChunkSize: 1);
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.FrameCount.Should().Be(2);
        message.GetFrame(0).ToArray().Should().Equal("A"u8.ToArray());
        message.GetFrame(1).ToArray().Should().Equal("B"u8.ToArray());
        message.Dispose();
    }

    [Fact]
    public async Task BorrowedMode_DeliversFrames_ValidDuringCallback()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("ping"u8.ToArray(), more: true),
            ZmtpTestData.Frame("pong"u8.ToArray())));
        using var parser = new ZmtpParser(source, new ZReceiveOptions { Policy = ZReceiveMode.Borrowed });
        var captured = new List<byte[]>();

        await parser.ParseAsync(new ZCallbackSink(borrowed: (view, _) =>
        {
            view.FrameCount.Should().Be(2);
            captured.Add(view.GetFrame(0).ToArray());
            captured.Add(view.GetFrame(1).ToArray());
            captured.Add(view.Whole.ToArray());
            return true;
        }));

        captured[0].Should().Equal("ping"u8.ToArray());
        captured[1].Should().Equal("pong"u8.ToArray());
        captured[2].Should().Equal("pingpong"u8.ToArray());
    }

    [Fact]
    public async Task PooledMessages_ReturnBuffersOnDispose()
    {
        using var pool = new CountingMemoryPool();
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("one"u8.ToArray()),
            ZmtpTestData.Frame("two"u8.ToArray())));
        var parser = new ZmtpParser(source, new ZReceiveOptions(), pool);
        var messages = new List<ZMessage>();

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            messages.Add(message);
            return true;
        }));

        messages.Should().HaveCount(2);
        foreach (var message in messages)
        {
            message.Dispose();
        }

        parser.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task Decide_ReturningOwned_DeliversOwnedMessage()
    {
        using var pool = new CountingMemoryPool();
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame("keep"u8.ToArray())));
        var parser = new ZmtpParser(source, new ZReceiveOptions(), pool);
        ZMessage? received = null;

        var sink = new ZCallbackSink(
            owned: (message, _) =>
            {
                received = message;
                return true;
            },
            decide: static (in ZReceiveContext _) =>
                new ZReceiveAction { Mode = ZReceiveMode.Owned, Contiguous = true });

        await parser.ParseAsync(sink);

        var message = received ?? throw new InvalidOperationException("expected an owned message");
        message.GetOrigin(0).Should().Be(ZBufferOrigin.Owned);
        message.GetFrame(0).ToArray().Should().Equal("keep"u8.ToArray());
        message.Dispose();
        parser.Dispose();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task FrameWithinLimit_IsContiguous()
    {
        var payload = Payload(100);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.TryGetContiguousFrame(0, out var memory).Should().BeTrue();
        memory.ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task FrameOverLimit_IsSegmented()
    {
        var payload = Payload(100_000);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.TryGetContiguousFrame(0, out _).Should().BeFalse();
        message.GetFrame(0).ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task LimitZero_SmallFrame_FitsSingleBlock()
    {
        var payload = Payload(100);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions { ContiguousFrameLimit = 0 });
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.TryGetContiguousFrame(0, out _).Should().BeTrue();
        message.GetFrame(0).ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task LimitZero_LargeFrame_IsSegmented()
    {
        var payload = Payload(20_000);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions { ContiguousFrameLimit = 0 });
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.TryGetContiguousFrame(0, out _).Should().BeFalse();
        message.GetFrame(0).ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task LongFrame_IsDelivered()
    {
        var payload = Payload(300);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        ZMessage? received = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            received = message;
            return true;
        }));

        var message = received ?? throw new InvalidOperationException("expected a message");
        message.TryGetContiguousFrame(0, out _).Should().BeTrue();
        message.GetFrame(0).ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Fact]
    public async Task BadGreetingSignature_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[0] = 0x00;
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting), new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedVersion_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[10] = 2;
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting), new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedMechanism_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        "CURVE"u8.CopyTo(greeting.AsSpan(12, 5));
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting), new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReservedFrameFlags_Throw()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], flagsOverride: 0b1000_0000)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameWithMore_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], more: true, command: true)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ErrorCommandInHandshake_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Error("boom")));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*boom*");
    }

    [Fact]
    public async Task CommandInsideMessage_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("A"u8.ToArray(), more: true),
            ZmtpTestData.Frame("PING"u8.ToArray(), command: true)));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var act = () => parser.ParseAsync(new ZCallbackSink()).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameInTraffic_IsSkipped()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("PING"u8.ToArray(), command: true),
            ZmtpTestData.Frame("hello"u8.ToArray())));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var delivered = 0;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            delivered++;
            message.Dispose();
            return true;
        }));

        delivered.Should().Be(1);
    }

    [Fact]
    public async Task EmptySource_ReturnsCleanly()
    {
        using var parser = new ZmtpParser(new ChunkedMemoryStream([]), new ZReceiveOptions());
        await parser.ParseAsync(new ZCallbackSink());
    }

    [Fact]
    public async Task EofMidFrame_EndsCleanly_WithoutMessage()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(),
            ZmtpTestData.Ready(),
            ZmtpTestData.Frame(new byte[10]));
        var truncated = wire[..^5];
        using var parser = new ZmtpParser(new ChunkedMemoryStream(truncated), new ZReceiveOptions());
        var delivered = 0;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            delivered++;
            message.Dispose();
            return true;
        }));

        delivered.Should().Be(0);
    }

    [Fact]
    public async Task EofAtBoundary_EndsAfterLastMessage()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame("last"u8.ToArray())));
        using var parser = new ZmtpParser(source, new ZReceiveOptions());
        var delivered = 0;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            delivered++;
            message.Dispose();
            return true;
        }));

        delivered.Should().Be(1);
    }

    [Fact]
    public async Task BorrowedPause_PausesAndResumes()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("one"u8.ToArray()),
            ZmtpTestData.Frame("two"u8.ToArray())));
        using var parser = new ZmtpParser(source, new ZReceiveOptions { Policy = ZReceiveMode.Borrowed });
        var firstDelivered = new TaskCompletionSource();
        var frames = new List<byte[]>();
        var sink = new ZCallbackSink(borrowed: (view, _) =>
        {
            frames.Add(view.GetFrame(0).ToArray());
            if (frames.Count == 1)
            {
                firstDelivered.TrySetResult();
                return false;
            }

            return true;
        });

        var parseTask = parser.ParseAsync(sink).AsTask();
        await firstDelivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(1);
        parseTask.IsCompleted.Should().BeFalse();

        parser.Resume();
        await parseTask.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(2);
        frames[1].Should().Equal("two"u8.ToArray());
    }

    [Fact]
    public async Task OwnedRejected_DisposesAndContinues()
    {
        using var pool = new CountingMemoryPool();
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("a"u8.ToArray()),
            ZmtpTestData.Frame("b"u8.ToArray())));
        var parser = new ZmtpParser(source, new ZReceiveOptions(), pool);
        ZMessage? accepted = null;

        await parser.ParseAsync(new ZCallbackSink(owned: (message, _) =>
        {
            if (message.GetFrame(0).FirstSpan[0] == (byte)'a')
            {
                return false;
            }

            accepted = message;
            return true;
        }));

        var message = accepted ?? throw new InvalidOperationException("expected accepted message");
        message.GetFrame(0).ToArray().Should().Equal("b"u8.ToArray());
        message.Dispose();
        parser.Dispose();
        pool.Outstanding.Should().Be(0);
    }
}
