using FluentAssertions;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

public sealed class ZmtpFrameEncoderTests
{
    [Fact]
    public async Task MultipartMessage_RoundTripsThroughParser()
    {
        using var message = MessageFactory.Multipart([.. "A"u8], [.. "B"u8], [.. "C"u8]);
        using var encodeTarget = new MemoryStream();
        var encoder = new ZmtpFrameEncoder(encodeTarget);
        await encoder.WriteMessageAsync(message);

        // The handshake's local writes are discarded by the read-only stream;
        // the fixture greeting + READY + the encoded message are all consumed
        // from the wire buffer.
        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(3);
        recorder.Frames[0].Should().Equal([.. "A"u8]);
        recorder.Frames[1].Should().Equal([.. "B"u8]);
        recorder.Frames[2].Should().Equal([.. "C"u8]);
        recorder.MoreFlags.Should().Equal(true, true, false);
    }

    [Fact]
    public async Task LongFrame_UsesLongEncoding()
    {
        var payload = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
        using var message = MessageFactory.Multipart(payload);
        using var encodeTarget = new MemoryStream();
        var encoder = new ZmtpFrameEncoder(encodeTarget);
        await encoder.WriteMessageAsync(message);

        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal(payload);
    }

    [Fact]
    public async Task SegmentedSingleFrame_RoundTripsAsOneFrame()
    {
        using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
        using var encodeTarget = new MemoryStream();
        var encoder = new ZmtpFrameEncoder(encodeTarget);
        await encoder.WriteMessageAsync(message);

        var wire = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready(), encodeTarget.ToArray());
        using var connection = new ZConnection(new ChunkedMemoryStream(wire));
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal([.. "hello!"u8]);
    }

    [Fact]
    public async Task CommandFrame_IsOneLogicalWrite_OfHeaderPlusBody()
    {
        // The command path (handshake READY/ERROR, mechanism commands) must
        // also write the frame as one logical write: short and long forms.
        var sink = new CaptureSink();
        var encoder = new ZmtpFrameEncoder(sink);

        await encoder.WriteCommandAsync("READY"u8.ToArray());
        var longBody = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
        await encoder.WriteCommandAsync(longBody);

        sink.Writes.Should().HaveCount(2);
        sink.Writes[0].SelectMany(segment => segment.ToArray())
            .Should().Equal(ZmtpTestData.Frame("READY"u8.ToArray(), command: true));
        sink.Writes[1].SelectMany(segment => segment.ToArray())
            .Should().Equal(ZmtpTestData.Frame(longBody, command: true));
    }

    [Fact]
    public async Task EachFrame_IsOneLogicalWrite_OfHeaderPlusAllSegments()
    {
        // 0015 section 6.1: a frame is produced as one sequence (header + all
        // segments) and handed to the sink in a single logical write, so the
        // socket sink can scatter-write it with one system call.
        var sink = new CaptureSink();
        var encoder = new ZmtpFrameEncoder(sink);

        using var message = MessageFactory.Multipart(
            [.. "AAA"u8], [.. "BBBB"u8], [.. "C"u8], [.. "DDDDDDD"u8]);
        await encoder.WriteMessageAsync(message);

        var expected = new (byte[] Frame, bool More)[]
        {
            ([.. "AAA"u8], true),
            ([.. "BBBB"u8], true),
            ([.. "C"u8], true),
            ([.. "DDDDDDD"u8], false),
        };

        // Each write concatenates to the exact wire frame (header + payload).
        sink.Writes.Should().HaveCount(4);
        for (var i = 0; i < 4; i++)
        {
            var wire = ZmtpTestData.Frame(expected[i].Frame, more: expected[i].More);
            sink.Writes[i].SelectMany(segment => segment.ToArray()).Should().Equal(wire);
        }

        // The first three frames carry the MORE flag; the last does not.
        sink.Writes[0][0].Span[0].Should().Be((byte)ZmtpFrameFlags.More);
        sink.Writes[1][0].Span[0].Should().Be((byte)ZmtpFrameFlags.More);
        sink.Writes[2][0].Span[0].Should().Be((byte)ZmtpFrameFlags.More);
        sink.Writes[3][0].Span[0].Should().Be((byte)ZmtpFrameFlags.None);
    }

    [Fact]
    public async Task SegmentedFrame_IsOneLogicalWrite_PreservingSegments()
    {
        var sink = new CaptureSink();
        var encoder = new ZmtpFrameEncoder(sink);

        using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
        await encoder.WriteMessageAsync(message);

        // One write per frame, one segment per original segment, with the
        // 2-byte short header first and the original MORE-less flags.
        sink.Writes.Should().HaveCount(1);
        sink.Writes[0].Should().HaveCount(4);
        sink.Writes[0][0].ToArray().Should().Equal([0x00, 0x06]);
        sink.Writes[0][1].ToArray().Should().Equal([.. "hel"u8]);
        sink.Writes[0][2].ToArray().Should().Equal([.. "lo"u8]);
        sink.Writes[0][3].ToArray().Should().Equal([.. "!"u8]);
    }
}
