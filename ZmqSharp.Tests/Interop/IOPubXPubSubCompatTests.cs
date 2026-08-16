using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Interop;

/// <summary>
/// Jupyter IOPub compatibility (the XPUB/SUB pair): a Jupyter kernel's IOPub
/// socket is an XPUB (Maieutics kernel: <c>XPublisherSocket</c>) and the
/// client's IOPub socket is a SUB (<c>SubscriberSocket</c>). libzmq's
/// compatibility matrix accepts SUB peers on XPUB and XPUB peers on SUB; the
/// ZmqSharp matrix must match for the migration (0025/0026 context).
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class IOPubXPubSubCompatTests
{
    [Fact]
    public async Task NetMQXPub_ZmqSharpSub_HandshakesAndDelivers()
    {
        // Kernel side: XPUB over TCP (Jupyter IOPub shape). Client side:
        // ZmqSharp SUB connected to it.
        using var xpub = new XPublisherSocket();
        xpub.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        xpub.Bind($"tcp://127.0.0.1:{port}");

        var received = Channel.CreateUnbounded<ZMessage>();
        await using var sub = new ZSubSocket(new ZSocketOptions { MessageSink = new TestSink(message => received.Writer.TryWrite(message)) });
        sub.Subscribe([.. "news"u8]);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await sub.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // Let the subscription frame propagate to NetMQ (it drops until subscribed).
        await Task.Delay(500);

        // The XPUB publishes the topic; the SUB client must receive it.
        xpub.SendFrame(Concat("news", "headline"));
        var message = await received.Reader.ReadAsync(CancellationToken.None).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));
        message[0].ToSequence().ToArray().Should().Equal(Concat("news", "headline"));
        message.Dispose();
    }

    [Fact]
    public async Task NetMQSub_ZmqSharpXPub_HandshakesAndBroadcasts()
    {
        // The reverse direction: ZmqSharp XPUB with a NetMQ SUB client
        // (libzmq XPUB accepts SUB peers).
        var subscriptions = Channel.CreateUnbounded<ZMessage>();
        await using var xpub = new ZXPubSocket(new ZSocketOptions { MessageSink = new TestSink(message => subscriptions.Writer.TryWrite(message)) });
        var port = InteropHelpers.GetFreePort();
        await xpub.BindAsync($"tcp://127.0.0.1:{port}");

        using var sub = new SubscriberSocket();
        sub.Options.Linger = TimeSpan.Zero;
        sub.Subscribe([.. "news"u8]);
        sub.Connect($"tcp://127.0.0.1:{port}");
        await Task.Delay(500); // let the subscription propagate

        await xpub.SendAsync(ZMessage.FromOwned(Concat("news", "headline")), new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
        var received = InteropHelpers.ReceiveFrame(sub, TimeSpan.FromSeconds(5));
        received.Should().Equal(Concat("news", "headline"));
    }

    [Fact]
    public async Task NetMQXPub_ZmqSharpXSub_HandshakesAndDelivers()
    {
        // XSUB client against the kernel's XPUB: libzmq accepts XSUB on XPUB
        // (the ZmqSharp XPUB predicate already covers it).
        var received = Channel.CreateUnbounded<ZMessage>();
        await using var xsub = new ZXSubSocket(new ZSocketOptions { MessageSink = new TestSink(message => received.Writer.TryWrite(message)) });
        xsub.Subscribe([.. "news"u8]);
        var port = InteropHelpers.GetFreePort();
        await xsub.BindAsync($"tcp://127.0.0.1:{port}");

        using var xpub = new XPublisherSocket();
        xpub.Options.Linger = TimeSpan.Zero;
        xpub.Connect($"tcp://127.0.0.1:{port}");
        await Task.Delay(500);

        xpub.SendFrame(Concat("news", "x"));
        var message = await received.Reader.ReadAsync(CancellationToken.None).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));
        message[0].ToSequence().ToArray().Should().Equal(Concat("news", "x"));
        message.Dispose();
    }

    private static byte[] Concat(string topic, string body)
    {
        var result = new byte[topic.Length + body.Length];
        System.Text.Encoding.ASCII.GetBytes(topic).CopyTo(result, 0);
        System.Text.Encoding.ASCII.GetBytes(body).CopyTo(result, topic.Length);
        return result;
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
