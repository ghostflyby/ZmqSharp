using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

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

        using var parser = new ZmtpParser(stream, new ZReceiveOptions());
        ZMessage? received = null;
        await parser.ParseAsync(new ZCallbackSink(owned: (m, _) =>
        {
            received = m;
            return true;
        }));

        var parsed = received ?? throw new InvalidOperationException("no message parsed");
        parsed.FrameCount.Should().Be(3);
        parsed.GetFrame(0).ToArray().Should().Equal("A"u8.ToArray());
        parsed.GetFrame(1).ToArray().Should().Equal("B"u8.ToArray());
        parsed.GetFrame(2).ToArray().Should().Equal("C"u8.ToArray());
        parsed.Dispose();
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

        using var parser = new ZmtpParser(stream, new ZReceiveOptions());
        ZMessage? received = null;
        await parser.ParseAsync(new ZCallbackSink(owned: (m, _) =>
        {
            received = m;
            return true;
        }));

        var parsed = received ?? throw new InvalidOperationException("no message parsed");
        parsed.GetFrame(0).ToArray().Should().Equal(payload);
        parsed.Dispose();
    }
}
