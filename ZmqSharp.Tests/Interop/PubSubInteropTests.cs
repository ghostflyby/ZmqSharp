using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Interop;

/// <summary>
///     PUB/SUB interop with the NetMQ libzmq-compatible implementation over TCP
///     (0006 section 5, 0013): broadcast outbound and topic-prefix subscription
///     filtering.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class PubSubInteropTests
{
    [Fact]
    public async Task ZmqSharpPub_NetMQSub_DeliversSubscribedTopics()
    {
        using var sub = new SubscriberSocket();
        sub.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        sub.Bind($"tcp://127.0.0.1:{port}");
        sub.Subscribe([.. "news"u8]);

        await using var pub = new ZPubSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await pub.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await pub.SendAsync(ZMessage.FromOwned(Concat("news", "item-1")), cts.Token);
        var received = InteropHelpers.ReceiveFrame(sub, TimeSpan.FromSeconds(5));
        received.Should().Equal(Concat("news", "item-1"));

        // A non-matching topic is not delivered.
        await pub.SendAsync(ZMessage.FromOwned(Concat("sport", "item")), cts.Token);
        sub.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(300), out _).Should().BeFalse();
    }

    [Fact]
    public async Task NetMQPub_ZmqSharpSub_FiltersBySubscription()
    {
        var channel = Channel.CreateUnbounded<ZMessage>();
        await using var sub = new ZSubSocket(new ZSocketOptions { MessageSink = new TestSink(message => channel.Writer.TryWrite(message)) });
        sub.Subscribe([.. "news"u8]);
        var port = InteropHelpers.GetFreePort();
        await sub.BindAsync($"tcp://127.0.0.1:{port}");

        using var pub = new PublisherSocket();
        pub.Options.Linger = TimeSpan.Zero;
        pub.Connect($"tcp://127.0.0.1:{port}");

        // The subscription frame must reach NetMQ before the publisher sends
        // anything (it drops until subscribed), so wait for propagation.
        await Task.Delay(500);

        pub.SendFrame(Concat("news", "headline"));
        var message = await channel.Reader.ReadAsync(CancellationToken.None).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));
        message.Count.Should().Be(1);
        message[0].ToSequence().ToArray().Should().Equal(Concat("news", "headline"));
        message.Dispose();

        // Unsubscribe propagates the 0x00 frame; the filter then drops the
        // topic and the publisher stops sending it.
        sub.Unsubscribe([.. "news"u8]);
        pub.SendFrame(Concat("sport", "score"));
        var drainTask = channel.Reader.ReadAsync(CancellationToken.None).AsTask();
        var idle = Task.Delay(300);
        var first = await Task.WhenAny(drainTask, idle);
        first.Should().Be(idle);
    }

    private static byte[] Concat(string topic, string payload)
    {
        var result = new byte[topic.Length + payload.Length];
        Encoding.ASCII.GetBytes(topic).CopyTo(result, 0);
        Encoding.ASCII.GetBytes(payload).CopyTo(result, topic.Length);
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
