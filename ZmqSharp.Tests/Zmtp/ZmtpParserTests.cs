using Xunit;
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

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal([.. "hello"u8], frame);
        Assert.False(recorder.MoreFlags[0]);
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

        Assert.Equal(3, recorder.Frames.Count);
        Assert.Equal([.. "A"u8], recorder.Frames[0]);
        Assert.Equal([.. "B"u8], recorder.Frames[1]);
        Assert.Equal([.. "C"u8], recorder.Frames[2]);
        Assert.Equal([true, true, false], recorder.MoreFlags);
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

        Assert.Equal(2, recorder.Frames.Count);
        Assert.Equal([.. "A"u8], recorder.Frames[0]);
        Assert.Equal([.. "B"u8], recorder.Frames[1]);
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

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal(payload, frame);
    }

    [Fact(Timeout = 10_000)]
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
            if (frames.Count != 1)
            {
                return true;
            }

            firstDelivered.TrySetResult();
            return false;
        });
        var session = await ZmtpTestRunner.EstablishAsync(connection)
                      ?? throw new InvalidOperationException("handshake failed");

        // The message-currency reader (0033): a consumer that stops reading
        // parks the stream - the pause the old pump's Resume() performed.
        var token = TestContext.Current.CancellationToken;
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parseTask = Task.Run(async () =>
        {
            using var messageSession = ZmtpTestRunner.CreateSession(session);
            while (await messageSession.ReadMessageAsync(token) is { } message)
            {
                foreach (var frame in message) await recorder.OnFrameAsync(frame, token);
                message.Dispose();
                if (frames.Count == 1) await resume.Task.WaitAsync(token);
            }
        }, token);
        await firstDelivered.Task.WaitAsync(token);
        Assert.Single(frames);
        Assert.False(parseTask.IsCompleted);

        resume.SetResult();
        await parseTask.WaitAsync(token);
        Assert.Equal(2, frames.Count);
        Assert.Equal([.. "two"u8], frames[1]);
    }

    [Fact(Timeout = 10_000)]
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
            if (frames.Count != 1)
            {
                return true;
            }

            firstSeen.TrySetResult();
            await release.Task; // pending ValueTask = backpressure
            return true;
        });
        var session = await ZmtpTestRunner.EstablishAsync(connection)
                      ?? throw new InvalidOperationException("handshake failed");

        // A pending sink task is backpressure by itself (0033): the read
        // loop awaits the sink before reading on.
        var token = TestContext.Current.CancellationToken;
        using var messageSession = ZmtpTestRunner.CreateSession(session);
        // ReSharper disable once AccessToDisposedClosure
        var parseTask = Task.Run(async () =>
        {
            while (await messageSession.ReadMessageAsync(token) is { } message)
            {
                foreach (var frame in message) await sink.OnFrameAsync(frame, token);
                message.Dispose();
            }
        }, token);
        await firstSeen.Task.WaitAsync(token);
        Assert.Single(frames);
        Assert.False(parseTask.IsCompleted);

        release.SetResult();
        await parseTask.WaitAsync(token);
        Assert.Equal(2, frames.Count);
        Assert.Equal([.. "two"u8], frames[1]);
    }

    [Fact]
    public async Task ReservedFrameFlags_Throw()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], flagsOverride: 0b1000_0000)));
        using var connection = new ZConnection(source);
        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder()));
    }

    [Fact]
    public async Task CommandFrameWithMore_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], true, true)));
        using var connection = new ZConnection(source);
        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder()));
    }

    [Fact]
    public void WireUnit_CommandRequiresSingleFrameWithoutMore()
    {
        using var multipart = MessageFactory.Multipart([1], [2]);
        Assert.Throws<ZeroMqProtocolException>(() => ZWireUnit.Command(multipart));

        using var more = new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(new byte[1], 0, 1), more: true)));
        Assert.Throws<ZeroMqProtocolException>(() => ZWireUnit.Command(more));

        using var single = ZMessage.FromOwned([1]);
        Assert.True(ZWireUnit.Command(single).IsCommand);
        Assert.False(ZWireUnit.Data(single).IsCommand);
    }

    [Fact]
    public void ByteLength_SumsSegmentsWithoutMaterializingASequence()
    {
        using var message = MessageFactory.SegmentedFrame([1, 2, 3], [4, 5], [6]);
        var frame = message[0];
        Assert.Equal(6, ZWireFrameLogic.ByteLength(frame));

        _ = ZWireFrameLogic.ByteLength(frame);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++) _ = ZWireFrameLogic.ByteLength(frame);
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
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

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal([.. "hello"u8], frame);
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

        Assert.Empty(recorder.Frames);
    }

    [Fact]
    public async Task EofAtBoundary_EndsAfterLastFrame()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([.. "last"u8])));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal([.. "last"u8], frame);
    }

    [Fact]
    public async Task CommandFrameInTraffic_MalformedCommandName_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([0], command: true)));
        using var connection = new ZConnection(source);

        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder()));
    }

    [Fact]
    public async Task CommandFrameInTraffic_ErrorCommand_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Error("terminate")));
        using var connection = new ZConnection(source);

        var ex = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder()));
        Assert.Contains("terminate", ex.Message);
    }

    /// <summary>Sink with an async frame handler, for pending-ValueTask backpressure tests.</summary>
    private sealed class AsyncSink(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrameAsync) : ITestFrameSink
    {
        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
        {
            return onFrameAsync(frame, token);
        }
    }
}
