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
}
