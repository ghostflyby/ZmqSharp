using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests;

/// <summary>
/// XPUB/XSUB interop with the NetMQ libzmq-compatible implementation over TCP
/// (0006 section 5, 0014): subscription observation and manual subscription
/// frames.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class XPubXSubInteropTests
{
    [Fact]
    public async Task ZmqSharpXPub_NetMQXSub_ObservesAndBroadcasts()
    {
        // NetMQ XSub -> ZmqSharp XPub: the subscription frame must reach our
        // XPub's sink, and a published topic must reach the NetMQ XSub.
        using var xsub = new XSubscriberSocket();
        xsub.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        xsub.Bind($"tcp://127.0.0.1:{port}");

        var subscriptions = Channel.CreateUnbounded<ZMessage>();
        await using var xpub = ZSocket.CreateXPub();
        xpub.BindMessageSink(new TestSink(message => subscriptions.Writer.TryWrite(message)));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await xpub.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // The NetMQ XSub announces its subscription via the wire frame;
        // our XPub observes it (0x01 + topic) and forwards it upstream.
        xsub.SendFrame([0x01, .. "news"u8]);
        var observed = await subscriptions.Reader.ReadAsync(cts.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        var observedFrame = observed[0].ToSequence().ToArray();
        observedFrame[0].Should().Be(0x01);
        observedFrame.AsSpan(1).SequenceEqual("news"u8).Should().BeTrue();
        observed.Dispose();

        // Publish a topic; the NetMQ XSub receives it (single frame).
        var payload = Encoding.ASCII.GetBytes("news").Concat(Encoding.ASCII.GetBytes("!")).ToArray();
        await xpub.SendAsync(ZMessage.FromOwned(payload), cts.Token);
        var received = InteropHelpers.ReceiveFrame(xsub, TimeSpan.FromSeconds(5));
        received.Should().Equal(payload);
    }

    [Fact]
    public async Task NetMQXPub_ZmqSharpXSub_ReceivesUnfiltered()
    {
        var received = Channel.CreateUnbounded<ZMessage>();
        await using var xsub = ZSocket.CreateXSubCallback();
        xsub.Subscribe("any"u8.ToArray());
        xsub.BindMessageSink(new TestSink(message => received.Writer.TryWrite(message)));
        var port = InteropHelpers.GetFreePort();
        await xsub.BindAsync($"tcp://127.0.0.1:{port}");

        using var xpub = new XPublisherSocket();
        xpub.Options.Linger = TimeSpan.Zero;
        xpub.Connect($"tcp://127.0.0.1:{port}");
        await Task.Delay(500); // let the subscription frame reach NetMQ

        // NetMQ's XPublisherSocket (like PUB) only forwards data matching a
        // received subscription; our XSUB delivers the frame unfiltered.
        var payload = Encoding.ASCII.GetBytes("any-thing").ToArray();
        xpub.SendFrame(payload);
        var message = await received.Reader.ReadAsync(CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        message[0].ToSequence().ToArray().Should().Equal(payload);
        message.Dispose();
        await xsub.DisposeAsync();
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}
