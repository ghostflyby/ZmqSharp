using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

public sealed class ZmtpFrameEncoderTests
{
    [Fact]
    public async Task MultipartMessage_RoundTripsThroughParser()
    {
        using var message = MessageFactory.Multipart("A"u8.ToArray(), "B"u8.ToArray(), "C"u8.ToArray());
        using var stream = new MemoryStream();
        await stream.WriteAsync(ZmtpTestData.Greeting());
        await stream.WriteAsync(ZmtpTestData.Ready());
        var encoder = new ZmtpFrameEncoder(stream);
        await encoder.WriteMessageAsync(message);
        stream.Position = 0;

        using var connection = new ZConnection(stream);
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(3);
        recorder.Frames[0].Should().Equal("A"u8.ToArray());
        recorder.Frames[1].Should().Equal("B"u8.ToArray());
        recorder.Frames[2].Should().Equal("C"u8.ToArray());
        recorder.MoreFlags.Should().Equal(true, true, false);
    }

    [Fact]
    public async Task LongFrame_UsesLongEncoding()
    {
        var payload = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
        using var message = MessageFactory.Multipart(payload);
        using var stream = new MemoryStream();
        await stream.WriteAsync(ZmtpTestData.Greeting());
        await stream.WriteAsync(ZmtpTestData.Ready());
        var encoder = new ZmtpFrameEncoder(stream);
        await encoder.WriteMessageAsync(message);
        stream.Position = 0;

        using var connection = new ZConnection(stream);
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal(payload);
    }

    [Fact]
    public async Task SegmentedSingleFrame_RoundTripsAsOneFrame()
    {
        using var message = MessageFactory.SegmentedFrame("hel"u8.ToArray(), "lo"u8.ToArray(), "!"u8.ToArray());
        using var stream = new MemoryStream();
        await stream.WriteAsync(ZmtpTestData.Greeting());
        await stream.WriteAsync(ZmtpTestData.Ready());
        var encoder = new ZmtpFrameEncoder(stream);
        await encoder.WriteMessageAsync(message);
        stream.Position = 0;

        using var connection = new ZConnection(stream);
        var recorder = new FrameRecorder();
        await ZmtpTestRunner.RunParserAsync(connection, recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal("hello!"u8.ToArray());
    }

    [Fact]
    public void BuildHandshake_LongCommand_UsesLongSizeHeader()
    {
        var body = new byte[300];
        var handshake = ZmtpFrameEncoder.BuildHandshake(body);

        handshake.Should().HaveCount(64 + 9 + body.Length);
        handshake[64].Should().Be((byte)(ZmtpFrameFlags.Command | ZmtpFrameFlags.LongSize));
        BinaryPrimitives.ReadInt64BigEndian(handshake.AsSpan(65, 8)).Should().Be(300);
    }
}
