using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// The framing's wire-format vectors (0033): the send path composes the
/// session (per-frame sends, the message loop) over the ZMTP framing, and
/// every frame leaves as one logical write of header + all body segments,
/// byte-identical to the retired encoder.
/// </summary>
public sealed class ZmtpFrameEncoderTests
{
    [Fact]
    public async Task MultipartMessage_RoundTripsThroughThePump()
    {
        using var message = MessageFactory.Multipart([.. "A"u8], [.. "B"u8], [.. "C"u8]);
        using var encodeTarget = new MemoryStream();
        using var sender = ZmtpTestRunner.CreateSession(new ZConnection(encodeTarget));
        await sender.SendAsync(message, TestContext.Current.CancellationToken);

        // The handshake's local writes are discarded by the read-only stream;
        // the fixture greeting + READY + the encoded message are all consumed
        // from the wire buffer.
        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        Assert.Equal(3, recorder.Frames.Count);
        Assert.Equal([.. "A"u8], recorder.Frames[0]);
        Assert.Equal([.. "B"u8], recorder.Frames[1]);
        Assert.Equal([.. "C"u8], recorder.Frames[2]);
        Assert.Equal([true, true, false], recorder.MoreFlags);
    }

    [Fact]
    public async Task LongFrame_UsesLongEncoding()
    {
        var payload = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
        using var message = MessageFactory.Multipart(payload);
        using var encodeTarget = new MemoryStream();
        using var sender = ZmtpTestRunner.CreateSession(new ZConnection(encodeTarget));
        await sender.SendAsync(message, TestContext.Current.CancellationToken);

        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal(payload, frame);
    }

    [Fact]
    public async Task SegmentedSingleFrame_RoundTripsAsOneFrame()
    {
        using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
        using var encodeTarget = new MemoryStream();
        using var sender = ZmtpTestRunner.CreateSession(new ZConnection(encodeTarget));
        await sender.SendAsync(message, TestContext.Current.CancellationToken);

        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        var frame = Assert.Single(recorder.Frames);
        Assert.Equal([.. "hello!"u8], frame);
    }

    [Fact]
    public async Task CommandFrame_IsOneLogicalWrite_OfHeaderPlusBody()
    {
        // The command path (handshake READY/ERROR, mechanism commands) must
        // also write the frame as one logical write: short and long forms.
        var sink = new CaptureSink();
        using var sender = ZmtpTestRunner.CreateSession(new SinkConnection(sink));

        await sender.SendCommandAsync("READY"u8.ToArray(), TestContext.Current.CancellationToken);
        var longBody = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
        await sender.SendCommandAsync(longBody, TestContext.Current.CancellationToken);

        Assert.Equal(2, sink.Writes.Count);
        Assert.Equal(ZmtpTestData.Frame([.. "READY"u8], command: true),
            sink.Writes[0].SelectMany(segment => segment.ToArray()));
        Assert.Equal(ZmtpTestData.Frame(longBody, command: true),
            sink.Writes[1].SelectMany(segment => segment.ToArray()));
    }

    [Fact]
    public async Task EachFrame_IsOneLogicalWrite_OfHeaderPlusAllSegments()
    {
        // 0015 section 6.1: a frame is produced as one sequence (header + all
        // segments) and handed to the sink in a single logical write, so the
        // socket sink can scatter-write it with one system call.
        var sink = new CaptureSink();
        using var sender = ZmtpTestRunner.CreateSession(new SinkConnection(sink));

        using var message = MessageFactory.Multipart(
            [.. "AAA"u8], [.. "BBBB"u8], [.. "C"u8], [.. "DDDDDDD"u8]);
        await sender.SendAsync(message, TestContext.Current.CancellationToken);

        var expected = new (byte[] Frame, bool More)[]
        {
            ([.. "AAA"u8], true),
            ([.. "BBBB"u8], true),
            ([.. "C"u8], true),
            ([.. "DDDDDDD"u8], false),
        };

        // Each write concatenates to the exact wire frame (header + payload).
        Assert.Equal(4, sink.Writes.Count);
        for (var i = 0; i < 4; i++)
        {
            var wire = ZmtpTestData.Frame(expected[i].Frame, more: expected[i].More);
            Assert.Equal(wire, sink.Writes[i].SelectMany(segment => segment.ToArray()));
        }

        // The first three frames carry the MORE flag; the last does not.
        Assert.Equal((byte)ZmtpFrameFlags.More, sink.Writes[0][0].Span[0]);
        Assert.Equal((byte)ZmtpFrameFlags.More, sink.Writes[1][0].Span[0]);
        Assert.Equal((byte)ZmtpFrameFlags.More, sink.Writes[2][0].Span[0]);
        Assert.Equal((byte)ZmtpFrameFlags.None, sink.Writes[3][0].Span[0]);
    }

    [Fact]
    public async Task SegmentedFrame_IsOneLogicalWrite_PreservingSegments()
    {
        var sink = new CaptureSink();
        using var sender = ZmtpTestRunner.CreateSession(new SinkConnection(sink));

        using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
        await sender.SendAsync(message, TestContext.Current.CancellationToken);

        // One write per frame, one segment per original segment, with the
        // 2-byte short header first and the original MORE-less flags.
        var write = Assert.Single(sink.Writes);
        Assert.Equal(4, write.Length);
        Assert.Equal([0x00, 0x06], write[0].ToArray());
        Assert.Equal([.. "hel"u8], write[1].ToArray());
        Assert.Equal([.. "lo"u8], write[2].ToArray());
        Assert.Equal([.. "!"u8], write[3].ToArray());
    }
}
