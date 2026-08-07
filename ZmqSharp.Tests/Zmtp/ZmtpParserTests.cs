using FluentAssertions;
using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

public sealed class ZmtpParserTests
{
    private static byte[] Payload(int length) => Enumerable.Range(0, length).Select(i => (byte)(i % 251)).ToArray();

    [Fact]
    public async Task SingleFrame_StreamsOneFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame("hello"u8.ToArray())));
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("hello"u8.ToArray());
        recorder.MoreFlags[0].Should().BeFalse();
    }

    [Fact]
    public async Task Multipart_StreamsFramesWithMoreFlags()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("A"u8.ToArray(), more: true),
            ZmtpTestData.Frame("B"u8.ToArray(), more: true),
            ZmtpTestData.Frame("C"u8.ToArray())));
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(3);
        recorder.Frames[0].Should().Equal("A"u8.ToArray());
        recorder.Frames[1].Should().Equal("B"u8.ToArray());
        recorder.Frames[2].Should().Equal("C"u8.ToArray());
        recorder.MoreFlags.Should().Equal(true, true, false);
    }

    [Fact]
    public async Task SplitReads_ByteByByte_StillParses()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("A"u8.ToArray(), more: true),
            ZmtpTestData.Frame("B"u8.ToArray()));
        var source = new ChunkedMemoryStream(wire, maxChunkSize: 1);
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(2);
        recorder.Frames[0].Should().Equal("A"u8.ToArray());
        recorder.Frames[1].Should().Equal("B"u8.ToArray());
    }

    [Fact]
    public async Task LongFrame_StreamsFullPayload()
    {
        var payload = Payload(300);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal(payload);
    }

    [Fact]
    public async Task Backpressure_PausesAndResumes()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("one"u8.ToArray()),
            ZmtpTestData.Frame("two"u8.ToArray())));
        using var parser = new ZmtpParser(source);
        var firstDelivered = new TaskCompletionSource();
        var frames = new List<byte[]>();
        var recorder = new FrameRecorder(onFrame: (frame, ct) =>
        {
            frames.Add(frame.Memory.ToArray());
            if (frames.Count == 1)
            {
                firstDelivered.TrySetResult();
                return false;
            }

            return true;
        });

        var parseTask = ZmtpTestRunner.RunParserAsync(parser, recorder);
        await firstDelivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(1);
        parseTask.IsCompleted.Should().BeFalse();

        parser.Resume();
        await parseTask.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(2);
        frames[1].Should().Equal("two"u8.ToArray());
    }

    [Fact]
    public async Task BadGreetingSignature_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[0] = 0x00;
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting));
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedVersion_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[10] = 2;
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting));
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedMechanism_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        "CURVE"u8.CopyTo(greeting.AsSpan(12, 5));
        using var parser = new ZmtpParser(new ChunkedMemoryStream(greeting));
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReservedFrameFlags_Throw()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], flagsOverride: 0b1000_0000)));
        using var parser = new ZmtpParser(source);
        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(parser, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameWithMore_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], more: true, command: true)));
        using var parser = new ZmtpParser(source);
        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(parser, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ErrorCommandInHandshake_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Error("boom")));
        using var parser = new ZmtpParser(source);
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*boom*");
    }

    [Fact]
    public async Task CommandFrameInTraffic_IsSkipped()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame("PING"u8.ToArray(), command: true),
            ZmtpTestData.Frame("hello"u8.ToArray())));
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("hello"u8.ToArray());
    }

    [Fact]
    public async Task EmptySource_ReturnsCleanly()
    {
        using var parser = new ZmtpParser(new ChunkedMemoryStream([]));
        await ZmtpTestRunner.RunParserAsync(parser, new FrameRecorder());
    }

    [Fact]
    public async Task EofMidFrame_EndsCleanly_WithoutFrame()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(),
            ZmtpTestData.Ready(),
            ZmtpTestData.Frame(new byte[10]));
        var truncated = wire[..^5];
        using var parser = new ZmtpParser(new ChunkedMemoryStream(truncated));
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().BeEmpty();
    }

    [Fact]
    public async Task EofAtBoundary_EndsAfterLastFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame("last"u8.ToArray())));
        using var parser = new ZmtpParser(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(parser, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("last"u8.ToArray());
    }
}
