using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

public sealed class RequestLifecycleIntegrationTests
{
    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CancelWaitingForReply_RetiresConnectionAndAllowsReconnect(TransportKind kind)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var cancelRequest = new CancellationTokenSource();
        var address = TestTransports.GetEndpoint(kind);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var respond = false;
        await using var responder = new ZRepSocket();
        responder.BindRequestHandler((context, token) =>
        {
            if (respond) return responder.SendReplyAsync(context, "reply"u8.ToArray(), token);
            received.TrySetResult();
            return ValueTask.CompletedTask;
        });
        await responder.BindAsync(address, timeout.Token);
        await using var requester = new ZReqSocket();
        await requester.ConnectAsync(address, timeout.Token);
        var request = requester.RequestAsync("first"u8.ToArray(), cancelRequest.Token);
        await received.Task.WaitAsync(timeout.Token);
        await cancelRequest.CancelAsync();
        await FluentActions.Awaiting(() => request.WaitAsync(timeout.Token)).Should().ThrowAsync<OperationCanceledException>();
        requester.PeerSnapshot.Should().BeEmpty();
        await requester.DisconnectAsync(address, timeout.Token);
        respond = true;
        await requester.ConnectAsync(address, timeout.Token);
        using var reply = await requester.RequestAsync("second"u8.ToArray(), timeout.Token);
        reply[0].ToSequence().ToArray().Should().Equal("reply"u8.ToArray());
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task MalformedReply_FaultsRequestAndEndsPeer(TransportKind kind)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
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
        await responder.BindAsync(address, timeout.Token);
        await requester.ConnectAsync(address, timeout.Token);
        var request = requester.RequestAsync("request"u8.ToArray(), timeout.Token);
        using var incoming = await received.Task.WaitAsync(timeout.Token);
        await responder.SendMalformedAsync(timeout.Token);
        await FluentActions.Awaiting(() => request.WaitAsync(timeout.Token)).Should().ThrowAsync<ZeroMqProtocolException>();
        (await ended.Task.WaitAsync(timeout.Token)).Should().BeOfType<ZeroMqProtocolException>();
        await requester.DisconnectAsync(address, timeout.Token);
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
