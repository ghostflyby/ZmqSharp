using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
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
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

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
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

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
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

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
            ZmtpTestData.Frame("one"u8.ToArray()),
            ZmtpTestData.Frame("two"u8.ToArray())));
        using var connection = new ZConnection(source);
        var firstDelivered = new TaskCompletionSource();
        var frames = new List<byte[]>();
        var recorder = new FrameRecorder(onFrame: (frame, _) =>
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
        using var parser = ZmtpTestRunner.CreateParser(connection, recorder);

        var parseTask = ZmtpTestRunner.RunParserAsync(parser);
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
        using var connection = new ZConnection(new ChunkedMemoryStream(greeting));
        using var parser = new ZmtpParser(connection);
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedVersion_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[10] = 2;
        using var connection = new ZConnection(new ChunkedMemoryStream(greeting));
        using var parser = new ZmtpParser(connection);
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task UnsupportedMechanism_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        "CURVE"u8.CopyTo(greeting.AsSpan(12, 5));
        using var connection = new ZConnection(new ChunkedMemoryStream(greeting));
        using var parser = new ZmtpParser(connection);
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReservedFrameFlags_Throw()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], flagsOverride: 0b1000_0000)));
        using var connection = new ZConnection(source);
        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameWithMore_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([1], more: true, command: true)));
        using var connection = new ZConnection(source);
        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ErrorCommandInHandshake_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Error("boom")));
        using var connection = new ZConnection(source);
        using var parser = new ZmtpParser(connection);
        Func<Task> act = () => parser.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*boom*");
    }

    [Fact]
    public async Task CommandFrameInTraffic_IsSkipped()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(),
            ZmtpTestData.Frame([4, (byte)'P', (byte)'I', (byte)'N', (byte)'G'], command: true),
            ZmtpTestData.Frame("hello"u8.ToArray())));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("hello"u8.ToArray());
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
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame("last"u8.ToArray())));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("last"u8.ToArray());
    }

    [Fact]
    public async Task ReadyWithSocketTypeMetadata_CompletesHandshake()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"), ZmtpTestData.Frame("ok"u8.ToArray())));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("ok"u8.ToArray());
    }

    [Fact]
    public async Task CommandName_ZeroLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([0]);
    }

    [Fact]
    public async Task CommandName_Truncated_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([10, (byte)'R', (byte)'E']);
    }

    [Fact]
    public async Task CommandName_NonAlphabetic_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([1, (byte)'1']);
    }

    [Fact]
    public async Task CommandName_MissingLengthPrefix_Throws()
    {
        await AssertHandshakeCommandRejectedAsync("READY\0"u8.ToArray());
    }

    [Fact]
    public async Task UnknownCommandDuringHandshake_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([4, (byte)'P', (byte)'I', (byte)'N', (byte)'G']);
    }

    [Fact]
    public async Task Metadata_EmptyPropertyName_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'R', (byte)'E', (byte)'A', (byte)'D', (byte)'Y', 0]);
    }

    [Fact]
    public async Task Metadata_InvalidPropertyNameCharacter_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'R', (byte)'E', (byte)'A', (byte)'D', (byte)'Y', 1, (byte)'!', 0, 0, 0, 0]);
    }

    [Fact]
    public async Task Metadata_TruncatedPropertyValueLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'R', (byte)'E', (byte)'A', (byte)'D', (byte)'Y', 1, (byte)'X', 0, 0]);
    }

    [Fact]
    public async Task Metadata_NegativePropertyValueLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'R', (byte)'E', (byte)'A', (byte)'D', (byte)'Y', 1, (byte)'X', 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    [Fact]
    public async Task Metadata_MissingSocketType_Throws()
    {
        await AssertHandshakeCommandRejectedAsync(ZmtpTestData.ReadyBodyWithProperties(("Identity", "")));
    }

    [Fact]
    public async Task Metadata_DuplicateSocketType_Throws()
    {
        await AssertHandshakeCommandRejectedAsync(ZmtpTestData.ReadyBodyWithProperties(
            ("Socket-Type", "PAIR"), ("socket-type", "PAIR")));
    }

    [Fact]
    public async Task Metadata_InvalidSocketTypeValue_Throws()
    {
        await AssertHandshakeCommandRejectedAsync(ZmtpTestData.ReadyBodyWithProperties(("Socket-Type", "FOO")));
    }

    [Fact]
    public async Task Metadata_SocketTypePropertyNameCaseInsensitive_CompletesHandshake()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithProperties(("socket-type", "PAIR"))));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().BeEmpty();
    }

    [Fact]
    public async Task Metadata_HugeValueLength_ThrowsProtocolError()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithRawProperty("Socket-Type"u8, int.MaxValue)));
        using var connection = new ZConnection(source);

        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReadyWithAdditionalMetadata_CompletesHandshake()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(),
            ZmtpTestData.ReadyWithProperties(("Socket-Type", "PAIR"), ("Identity", "abc")),
            ZmtpTestData.Frame("ok"u8.ToArray())));
        using var connection = new ZConnection(source);
        var recorder = new FrameRecorder();

        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("ok"u8.ToArray());
    }

    [Fact]
    public async Task ErrorCommand_EmptyReason_ThrowsProtocolException()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 0]);
    }

    [Fact]
    public async Task ErrorCommand_MissingReasonLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R']);
    }

    [Fact]
    public async Task ErrorCommand_ReasonLengthMismatch_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 3, (byte)'x']);
    }

    [Fact]
    public async Task ErrorCommand_ReasonWithNonVisibleCharacter_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 1, 0x08]);
    }

    [Fact]
    public async Task CommandFrameInTraffic_MalformedCommandName_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"), ZmtpTestData.Frame([0], command: true)));
        using var connection = new ZConnection(source);

        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task CommandFrameInTraffic_ErrorCommand_Throws()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"), ZmtpTestData.Error("terminate")));
        using var connection = new ZConnection(source);

        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>().WithMessage("*terminate*");
    }

    [Fact]
    public async Task CommandFrame_AtMaxCommandSize_IsNotRejectedBySizeCheck()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), CommandFrameHeader(1 << 20)));
        using var connection = new ZConnection(source);

        // No body follows the header, so the handshake ends at EOF; the size
        // check must not reject the boundary value itself.
        await ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
    }

    [Fact]
    public async Task CommandFrame_OnePastMaxCommandSize_ThrowsBeforeBodyRead()
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), CommandFrameHeader((1 << 20) + 1)));
        using var connection = new ZConnection(source);

        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>().WithMessage("*exceeds maximum size*");
    }

    private static byte[] CommandFrameHeader(long size)
    {
        var header = new byte[9];
        header[0] = (byte)(ZmtpFrameFlags.Command | ZmtpFrameFlags.LongSize);
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1), size);
        return header;
    }

    private static async Task AssertHandshakeCommandRejectedAsync(byte[] body)
    {
        var source = new ChunkedMemoryStream(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Frame(body, command: true)));
        using var connection = new ZConnection(source);

        Func<Task> act = () => ZmtpTestRunner.RunParserAsync(connection, new FrameRecorder());
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }
}
