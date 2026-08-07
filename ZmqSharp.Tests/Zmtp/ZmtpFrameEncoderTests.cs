using FluentAssertions;
using Xunit;
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

        using var parser = new ZmtpParser(stream);
        var recorder = new FrameRecorder();
        await parser.ParseAsync(recorder);

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

        using var parser = new ZmtpParser(stream);
        var recorder = new FrameRecorder();
        await parser.ParseAsync(recorder);

        recorder.Frames.Should().HaveCount(1);
        recorder.Frames[0].Should().Equal(payload);
    }
}
