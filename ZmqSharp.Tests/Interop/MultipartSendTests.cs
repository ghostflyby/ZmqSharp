using System.Buffers;
using System.Threading.Channels;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
/// Multipart send surfaces (0026): the copy-input overloads on the direct
/// types, ROUTER's identity-addressed multipart, and REQ/REP multipart
/// requests and replies - verified over real sockets including NetMQ.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class MultipartSendTests
{
    [Fact(Timeout = 15_000)]
    public async Task Dealer_SendsMultipart_CopyEnumerable_And_RoundTripsThroughNetMQRouter()
    {
        using var router = new RouterSocket();
        router.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        router.Bind($"tcp://127.0.0.1:{port}");

        await using var dealer = new ZDealerSocket();
        var token = TestContext.Current.CancellationToken;
        await dealer.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // Jupyter-shaped five-frame message, one frame per element; a
        // byte[][] binds the IEnumerable<byte[]> overload directly.
        byte[][] frames =
        [
            [.. "identity"u8],
            [.. "hmac"u8],
            [.. "header"u8],
            [.. "parent"u8],
            [.. "content"u8]
        ];
        await dealer.SendAsync(frames, token);

        var message = new NetMQMessage();
        Assert.True(router.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref message));
        Assert.NotNull(message);
        // The NetMQ ROUTER prefixes the peer's routing id, so the wire frame
        // count is the five payload frames plus the identity frame.
        Assert.Equal(6, message.FrameCount);
        Assert.NotEmpty(message[0].ToByteArray());
        Assert.Equal([.. "identity"u8], message[1].ToByteArray());
        Assert.Equal([.. "hmac"u8], message[2].ToByteArray());
        Assert.Equal([.. "header"u8], message[3].ToByteArray());
        Assert.Equal([.. "parent"u8], message[4].ToByteArray());
        Assert.Equal([.. "content"u8], message[5].ToByteArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task Pair_SendsReadOnlySequence_AsSingleFrame()
    {
        var token = TestContext.Current.CancellationToken;
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        var port = InteropHelpers.GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // A two-segment sequence is one frame with non-contiguous content.
        var first = "abc"u8.ToArray();
        var second = "def"u8.ToArray();
        var seg1 = new SequenceSegment(first, 0);
        var seg2 = new SequenceSegment(second, first.Length);
        seg1.Link = seg2;
        var sequence = new ReadOnlySequence<byte>(seg1, 0, seg2, seg2.Memory.Length);

        await client.SendAsync(sequence, token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(message);
        var frame = Assert.Single(message.Value);
        Assert.Equal([.. "abcdef"u8], frame.ToSequence().ToArray());
        message.Value.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task ReqRep_MultipartRequestAndReply()
    {
        var token = TestContext.Current.CancellationToken;
        await using var rep = new ZRepSocket();
        await using var req = new ZReqSocket();
        var port = InteropHelpers.GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}", token);

        rep.BindRequestHandler((context, replyToken) => rep.SendReplyAsync(context, [new ReadOnlyMemory<byte>([.. "reply"u8])], replyToken));

        await req.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        ReadOnlyMemory<byte>[] request =
        [
            (byte[])[.. "part-1"u8],
            (byte[])[.. "part-2"u8]
        ];
        var reply = await req.RequestAsync(request, token);

        var item = Assert.Single(reply);
        Assert.Equal([.. "reply"u8], item.ToSequence().ToArray());
        reply.Dispose();
    }

    [Fact(Timeout = 15_000)]
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
        var token = TestContext.Current.CancellationToken;
        await router.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        dealer.SendFrame([.. "ping"u8]);
        var routed = await routedMessage.Task.WaitAsync(token);
        var identity = routed[0].ToSequence().ToArray();
        routed.Dispose();

        // Multipart reply addressed by the peer's routing identity.
        ReadOnlyMemory<byte>[] replyFrames = ["part-1"u8.ToArray(), "part-2"u8.ToArray()];
        await router.SendAsync(identity, replyFrames, token);
        var message = new NetMQMessage();
        Assert.True(dealer.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref message));
        Assert.NotNull(message);
        Assert.Equal(2, message.FrameCount);
        Assert.Equal([.. "part-1"u8], message[0].ToByteArray());
        Assert.Equal([.. "part-2"u8], message[1].ToByteArray());
    }

    private static async Task<ZMessage?> ReadMessageAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan timeout,
        CancellationToken token)
    {
        using var window = CancellationTokenSource.CreateLinkedTokenSource(token);
        window.CancelAfter(timeout);
        try
        {
            return await reader.ReadAsync(window.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return null;
        }
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
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
