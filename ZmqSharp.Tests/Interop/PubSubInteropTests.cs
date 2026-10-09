using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
///     PUB/SUB interop with the NetMQ libzmq-compatible implementation over TCP
///     (0006 section 5, 0013): broadcast outbound and topic-prefix subscription
///     filtering.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class PubSubInteropTests
{
    [Fact(Timeout = 20_000)]
    public async Task ZmqSharpPub_NetMQSub_DeliversSubscribedTopics()
    {
        using var sub = new SubscriberSocket();
        sub.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        sub.Bind($"tcp://127.0.0.1:{port}");
        sub.Subscribe([.. "news"u8]);

        await using var pub = new ZPubSocket();
        var token = TestContext.Current.CancellationToken;
        await pub.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        await pub.SendAsync(ZMessage.FromOwned(Concat("news", "item-1")), token);
        var received = InteropHelpers.ReceiveFrame(sub, TimeSpan.FromSeconds(5));
        received.Should().Equal(Concat("news", "item-1"));

        // A non-matching topic is not delivered.
        await pub.SendAsync(ZMessage.FromOwned(Concat("sport", "item")), token);
        sub.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(300), out _).Should().BeFalse();
    }

    [Fact(Timeout = 20_000)]
    public async Task NetMQPub_ZmqSharpSub_FiltersBySubscription()
    {
        var channel = Channel.CreateUnbounded<ZMessage>();
        await using var sub = new ZSubSocket(new ZSocketOptions { MessageSink = new TestSink(message => channel.Writer.TryWrite(message)) });
        sub.Subscribe([.. "news"u8]);
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await sub.BindAsync($"tcp://127.0.0.1:{port}", token);

        using var pub = new PublisherSocket();
        pub.Options.Linger = TimeSpan.Zero;
        pub.Connect($"tcp://127.0.0.1:{port}");

        // The subscription frame must reach NetMQ before the publisher sends
        // anything (it drops until subscribed), so wait for propagation.
        await Task.Delay(500, token);

        pub.SendFrame(Concat("news", "headline"));
        var message = await channel.Reader.ReadAsync(token);
        message.Count.Should().Be(1);
        message[0].ToSequence().ToArray().Should().Equal(Concat("news", "headline"));
        message.Dispose();

        // Unsubscribe propagates the 0x00 frame; the filter then drops the
        // topic and the publisher stops sending it.
        sub.Unsubscribe([.. "news"u8]);
        pub.SendFrame(Concat("sport", "score"));
        var drainTask = channel.Reader.ReadAsync(token).AsTask();
        var idle = Task.Delay(300, token);
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
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}
