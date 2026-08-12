using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// Traffic-only parser tests (0016 section 10): the fixture streams include
/// the peer greeting + READY, consumed by the NULL handshake before the
/// parser runs. Handshake validation lives in ZmtpHandshakeTests.
/// </summary>
public sealed class ZmtpParserTests
{
    private static byte[] Payload(int length)
    {
        return [.. Enumerable.Range(0, length).Select(i => (byte)(i % 251))];
    }

    [Fact]
    public async Task SingleFrame_StreamsOneFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([.. "hello"u8])));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal([.. "hello"u8]);
        recorder.MoreFlags[0].Should().BeFalse();
    }

    [Fact]
    public async Task Multipart_StreamsFramesWithMoreFlags()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([.. "A"u8], true),
            ZmtpTestData.Frame([.. "B"u8], true),
            ZmtpTestData.Frame([.. "C"u8])));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(3);
        recorder.Frames[0].Should().Equal([.. "A"u8]);
        recorder.Frames[1].Should().Equal([.. "B"u8]);
        recorder.Frames[2].Should().Equal([.. "C"u8]);
        recorder.MoreFlags.Should().Equal(true, true, false);
    }

    [Fact]
    public async Task SplitReads_ByteByByte_StillParses()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([.. "A"u8], true),
            ZmtpTestData.Frame([.. "B"u8]));
        var source = new ChunkedMemoryStream(wire, 1);
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(2);
        recorder.Frames[0].Should().Equal([.. "A"u8]);
        recorder.Frames[1].Should().Equal([.. "B"u8]);
    }

    [Fact]
    public async Task LongFrame_StreamsFullPayload()
    {
        var payload = Payload(300);
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame(payload)));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal(payload);
    }

    [Fact]
    public async Task Backpressure_PausesAndResumes()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([.. "one"u8]),
            ZmtpTestData.Frame([.. "two"u8])));
        using var connection = new ZConnection(source);
        var firstDelivered = new TaskCompletionSource();
        var frames = new List<byte[]>();
        var recorder = new FrameRecorder((frame, _) =>
        {
            frame.TryGetValue(out ZSegment segment);
            frames.Add(segment.Memory.ToArray());
            if (frames.Count == 1)
            {
                firstDelivered.TrySetResult();
                return false;
            }

            return true;
        });
        var session = await ZmtpTestRunner.EstablishAsync(connection);
        using var parser = ZmtpTestRunner.CreateParser(session is { } s ? s : throw new InvalidOperationException("handshake failed"), recorder);

        var parseTask = parser.ParseAsync().AsTask();
        await firstDelivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(1);
        parseTask.IsCompleted.Should().BeFalse();

        parser.Resume();
        await parseTask.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(2);
        frames[1].Should().Equal([.. "two"u8]);
    }

    [Fact]
    public async Task AsyncSink_PendingTask_PausesPumpUntilReleased()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([.. "one"u8]),
            ZmtpTestData.Frame([.. "two"u8])));
        using var connection = new ZConnection(source);
        var release = new TaskCompletionSource();
        var firstSeen = new TaskCompletionSource();
        var frames = new List<byte[]>();
        var sink = new AsyncSink(async (frame, _) =>
        {
            frame.TryGetValue(out ZSegment segment);
            frames.Add(segment.Memory.ToArray());
            if (frames.Count == 1)
            {
                firstSeen.TrySetResult();
                await release.Task; // pending ValueTask = backpressure
            }

            return true;
        });
        var session = await ZmtpTestRunner.EstablishAsync(connection);
        var parser = ZmtpTestRunner.CreateParser(session is { } s ? s : throw new InvalidOperationException("handshake failed"), sink);

        var parseTask = parser.ParseAsync().AsTask();
        await firstSeen.Task.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(1);
        parseTask.IsCompleted.Should().BeFalse();

        release.SetResult();
        await parseTask.WaitAsync(TimeSpan.FromSeconds(5));
        frames.Should().HaveCount(2);
        frames[1].Should().Equal([.. "two"u8]);
    }

    [Fact]
    public async Task ReservedFrameFlags_Throw()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], flagsOverride: 0b1000_0000)));
        using var connection = new ZConnection(source);
        var act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameWithMore_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], true, true)));
        using var connection = new ZConnection(source);
        var act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameInTraffic_IsSkipped()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([4, (byte)'P', (byte)'I', (byte)'N', (byte)'G'], command: true),
            ZmtpTestData.Frame([.. "hello"u8])));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal([.. "hello"u8]);
    }

    [Fact]
    public async Task EmptySource_ReturnsCleanly()
    {
        using var connection = new ZConnection(new ChunkedMemoryStream([]));
        await ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
    }

    [Fact]
    public async Task EofMidFrame_EndsCleanly_WithoutFrame()
    {
        var wire = ZmtpTestData.Concat(
            ZmtpTestData.Greeting(),
            ZmtpTestData.Ready(),
            ZmtpTestData.Frame(new byte[10]));
        var truncated = wire[..^5];
        using var connection = new ZConnection(new ChunkedMemoryStream(truncated));
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().BeEmpty();
    }

    [Fact]
    public async Task EofAtBoundary_EndsAfterLastFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([.. "last"u8])));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal([.. "last"u8]);
    }

    [Fact]
    public async Task CommandFrameInTraffic_MalformedCommandName_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([0], command: true)));
        using var connection = new ZConnection(source);

        var act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameInTraffic_ErrorCommand_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Error("terminate")));
        using var connection = new ZConnection(source);

        var act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>().WithMessage("*terminate*");
    }

    /// <summary>Sink with an async frame handler, for pending-ValueTask backpressure tests.</summary>
    private sealed class AsyncSink(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrameAsync) : IZMessageSink
    {
        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
        {
            return onFrameAsync(frame, token);
        }

        public void OnConnectionEnded()
        {
        }
    }
}
