using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;
using ZmqSharp;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Interop;

/// <summary>
/// Multipart send surfaces (0026): the copy-input overloads on the direct
/// types, ROUTER's identity-addressed multipart, and REQ/REP multipart
/// requests and replies - verified over real sockets including NetMQ.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class MultipartSendTests
{
    [Fact]
    public async Task Dealer_SendsMultipart_CopyEnumerable_And_RoundTripsThroughNetMQRouter()
    {
        using var router = new RouterSocket();
        router.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        router.Bind($"tcp://127.0.0.1:{port}");

        await using var dealer = new ZDealerSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await dealer.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // Jupyter-shaped five-frame message, one frame per element.
        ReadOnlyMemory<byte>[] frames =
        [
            "identity"u8.ToArray(),
            "hmac"u8.ToArray(),
            "header"u8.ToArray(),
            "parent"u8.ToArray(),
            "content"u8.ToArray(),
        ];
        await dealer.SendAsync(frames, cts.Token);

        var message = new NetMQMessage();
        router.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref message).Should().BeTrue();
        message.Should().NotBeNull();
        // The NetMQ ROUTER prefixes the peer's routing id, so the wire frame
        // count is the five payload frames plus the identity frame.
        message.FrameCount.Should().Be(6);
        message[0].ToByteArray().Should().NotBeEmpty();
        message[1].ToByteArray().Should().Equal([.. "identity"u8]);
        message[2].ToByteArray().Should().Equal([.. "hmac"u8]);
        message[3].ToByteArray().Should().Equal([.. "header"u8]);
        message[4].ToByteArray().Should().Equal([.. "parent"u8]);
        message[5].ToByteArray().Should().Equal([.. "content"u8]);
    }

    [Fact]
    public async Task Pair_SendsReadOnlySequence_AsSingleFrame()
    {
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        var port = InteropHelpers.GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // A two-segment sequence is one frame with non-contiguous content.
        var first = "abc"u8.ToArray();
        var second = "def"u8.ToArray();
        var seg1 = new SequenceSegment(first, 0);
        var seg2 = new SequenceSegment(second, first.Length);
        seg1.Link = seg2;
        var sequence = new ReadOnlySequence<byte>(seg1, 0, seg2, seg2.Memory.Length);

        await client.SendAsync(sequence, cts.Token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5));
        message.Should().NotBeNull();
        message.Value.Count.Should().Be(1);
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "abcdef"u8]);
        message.Value.Dispose();
    }

    [Fact]
    public async Task ReqRep_MultipartRequestAndReply()
    {
        await using var rep = new ZRepSocket();
        await using var req = new ZReqSocket();
        var port = InteropHelpers.GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");

        rep.BindRequestHandler((context, token) => rep.SendReplyAsync(context, new[] { new ReadOnlyMemory<byte>("reply"u8.ToArray()) }, token));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await req.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        ReadOnlyMemory<byte>[] request =
        [
            "part-1"u8.ToArray(),
            "part-2"u8.ToArray(),
        ];
        var reply = await req.RequestAsync(request, cts.Token);

        reply.Count.Should().Be(1);
        reply[0].ToSequence().ToArray().Should().Equal([.. "reply"u8]);
        reply.Dispose();
    }

    [Fact]
    public async Task Router_SendsMultipart_ByIdentity()
    {
        using var dealer = new DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        var routedMessage = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var router = new ZRouterSocket(new ZSocketOptions
        {
            MessageSink = new TestSink(message => routedMessage.TrySetResult(message))
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await router.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        dealer.SendFrame([.. "ping"u8]);
        var routed = await routedMessage.Task.WaitAsync(cts.Token);
        var identity = routed[0].ToSequence().ToArray();
        routed.Dispose();

        // Multipart reply addressed by the peer's routing identity.
        ReadOnlyMemory<byte>[] replyFrames = ["part-1"u8.ToArray(), "part-2"u8.ToArray()];
        await router.SendAsync(identity, replyFrames, cts.Token);
        var message = new NetMQMessage();
        dealer.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref message).Should().BeTrue();
        message.Should().NotBeNull();
        message.FrameCount.Should().Be(2);
        message[0].ToByteArray().Should().Equal([.. "part-1"u8]);
        message[1].ToByteArray().Should().Equal([.. "part-2"u8]);
    }

    private static async Task<ZMessage?> ReadMessageAsync(ChannelReader<ZMessage> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
    {
        public SequenceSegment(byte[] memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public ReadOnlySequenceSegment<byte>? Link
        {
            set => Next = value;
        }
    }
}
