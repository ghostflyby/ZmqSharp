using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;

namespace ZmqSharp.Tests.Sockets;

public sealed class RequestLifecycleIntegrationTests
{
    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CancelWaitingForReply_RetiresConnectionAndAllowsReconnect(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        using var cancelRequest = new CancellationTokenSource();
        var address = TestTransports.GetEndpoint(kind);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var respond = false;
        await using var responder = new ZRepSocket();
        responder.BindRequestHandler((context, replyToken) =>
        {
            if (respond) return responder.SendReplyAsync(context, "reply"u8.ToArray(), replyToken);
            received.TrySetResult();
            return ValueTask.CompletedTask;
        });
        await responder.BindAsync(address, token);
        await using var requester = new ZReqSocket();
        await requester.ConnectAsync(address, token);
        var request = requester.RequestAsync("first"u8.ToArray(), cancelRequest.Token);
        await received.Task.WaitAsync(token);
        await cancelRequest.CancelAsync();
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<OperationCanceledException>();
        requester.PeerSnapshot.Should().BeEmpty();
        await requester.DisconnectAsync(address, token);
        respond = true;
        await requester.ConnectAsync(address, token);
        using var reply = await requester.RequestAsync("second"u8.ToArray(), token);
        reply[0].ToSequence().ToArray().Should().Equal("reply"u8.ToArray());
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task MalformedReply_FaultsRequestAndEndsPeer(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        using var pool = new CountingMemoryPool();
        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ended = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var address = TestTransports.GetEndpoint(kind);
        await using var responder = new RawRepSocket(new ZSocketOptions
        {
            MessageSink = new RequestCaptureSink(received)
        });
        await using var requester = new ZReqSocket(new ZSocketOptions { Pool = pool });
        requester.PeerEnded += (_, failure) => ended.TrySetResult(failure);
        await responder.BindAsync(address, token);
        await requester.ConnectAsync(address, token);
        var request = requester.RequestAsync("request"u8.ToArray(), token);
        using var incoming = await received.Task.WaitAsync(token);
        await responder.SendMalformedAsync(token);
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<ZeroMqProtocolException>();
        (await ended.Task.WaitAsync(token)).Should().BeOfType<ZeroMqProtocolException>();
        await requester.DisconnectAsync(address, token);
        pool.Outstanding.Should().Be(0);
    }

    private sealed class RequestCaptureSink(TaskCompletionSource<ZMessage> received) : IPatternSink
    {
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token)
        {
            if (!received.TrySetResult(message)) message.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RawRepSocket(ZSocketOptions options)
        : ZSocketBase(options, new ZSinglePeerDispatch(), ZSocketTypes.Rep)
    {
        public ValueTask SendMalformedAsync(CancellationToken token)
            => SendAsyncCore(ZMessage.Copy("no delimiter"u8.ToArray()), token);
    }
}
