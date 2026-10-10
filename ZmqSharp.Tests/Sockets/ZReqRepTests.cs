using System.Buffers;
using System.Net.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
///     REQ/REP pattern tests (0010): strict alternation, round-robin,
///     delimiter framing, directed replies, and peer-retirement behavior. The
///     real-socket cases run over both TCP and ipc transports (0015 section
///     5.4); the local-only cases stay single-run.
/// </summary>
public sealed class ZReqRepTests
{
    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReqRep_RoundTripsRequestAndReply(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var rep = new ZRepSocket();
        await rep.BindAsync(endpoint, token);
        rep.BindRequestHandler(async (context, handlerToken) =>
        {
            var frame = Assert.Single(context);
            var payload = frame.ToSequence().ToArray();
            await rep.SendReplyAsync(context, ZMessage.FromOwned(payload), handlerToken);
        });

        await using var req = new ZReqSocket();
        await req.ConnectAsync(endpoint, token);
        var reply = await req.RequestAsync(ZMessage.FromOwned([.. "ping"u8]), token);

        var frame = Assert.Single(reply);
        Assert.Equal([.. "ping"u8], frame.ToSequence().ToArray());
        reply.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReqRep_MultipartRequestAndReply(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var rep = new ZRepSocket();
        await rep.BindAsync(endpoint, token);
        rep.BindRequestHandler((context, handlerToken) =>
        {
            Assert.Equal(2, context.Count);
            return rep.SendReplyAsync(context, MessageFactory.Multipart([.. "x"u8], [.. "y"u8]), handlerToken);
        });

        await using var req = new ZReqSocket();
        await req.ConnectAsync(endpoint, token);
        var reply = await req.RequestAsync(MessageFactory.Multipart([.. "a"u8], [.. "b"u8]), token);

        Assert.Equal(2, reply.Count);
        Assert.Equal([.. "x"u8], reply[0].ToSequence().ToArray());
        Assert.Equal([.. "y"u8], reply[1].ToSequence().ToArray());
        reply.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task Req_StrictAlternation_ThrowsWhileInFlight(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var rep = new ZRepSocket();
        await rep.BindAsync(endpoint, token);
        rep.BindRequestHandler(async (context, handlerToken) =>
        {
            await release.Task.WaitAsync(handlerToken);
            await rep.SendReplyAsync(context, ZMessage.FromOwned([.. "ok"u8]), handlerToken);
        });

        await using var req = new ZReqSocket();
        await req.ConnectAsync(endpoint, token);

        var first = req.RequestAsync(ZMessage.FromOwned([.. "1"u8]), token);

        await Assert.ThrowsAsync<InvalidOperationException>(() => req.RequestAsync(ZMessage.FromOwned([.. "2"u8]), token));

        release.TrySetResult();
        var reply = await first;
        reply.Dispose();

        // After the reply lands the gate reopens: a second request succeeds.
        var second = await req.RequestAsync(ZMessage.FromOwned([.. "3"u8]), token);
        second.Dispose();
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task Req_PeerClosesBeforeReply_FaultsRequestAndFreesGate(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(kind);
        var rep = new ZRepSocket();
        await rep.BindAsync(endpoint, token);
        // No handler bound: requests arrive and are dropped, so the reply
        // never comes and the peer stays alive until we close it.

        await using var req = new ZReqSocket();
        await req.ConnectAsync(endpoint, token);
        var requestTask = req.RequestAsync(ZMessage.FromOwned([.. "1"u8]), token);

        // Closing the REP peer retires the REQ's current connection and
        // faults the in-flight request (0010 section 2).
        await rep.DisposeAsync();

        var ex = await Record.ExceptionAsync(() => requestTask.WaitAsync(token));
        Assert.NotNull(ex);
        Assert.True(ex is IOException or SocketException or ObjectDisposedException);
    }

    [Fact]
    public async Task Req_NoPeer_Throws()
    {
        var token = TestContext.Current.CancellationToken;
        await using var req = new ZReqSocket();
        await Assert.ThrowsAsync<InvalidOperationException>(() => req.RequestAsync(ZMessage.FromOwned([.. "x"u8]), token));
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task Rep_RoutesReplyToOriginatingPeer_WithTwoPeers(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var rep = new ZRepSocket();
        await rep.BindAsync(endpoint, token);
        rep.BindRequestHandler(async (context, handlerToken) =>
        {
            var payload = context[0].ToSequence().ToArray();
            await rep.SendReplyAsync(context, ZMessage.FromOwned(payload), handlerToken);
        });

        await using var reqA = new ZReqSocket();
        await using var reqB = new ZReqSocket();
        await reqA.ConnectAsync(endpoint, token);
        await reqB.ConnectAsync(endpoint, token);

        var replyA = await reqA.RequestAsync(ZMessage.FromOwned([.. "a"u8]), token);
        Assert.Equal([.. "a"u8], replyA[0].ToSequence().ToArray());
        replyA.Dispose();

        var replyB = await reqB.RequestAsync(ZMessage.FromOwned([.. "b"u8]), token);
        Assert.Equal([.. "b"u8], replyB[0].ToSequence().ToArray());
        replyB.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task Req_RoundRobinsAcrossTwoPeers(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var endpointA = TestTransports.GetEndpoint(kind);
        var endpointB = TestTransports.GetEndpoint(kind);
        await using var repA = new ZRepSocket();
        await using var repB = new ZRepSocket();
        await repA.BindAsync(endpointA, token);
        await repB.BindAsync(endpointB, token);
        var countA = 0;
        var countB = 0;
        repA.BindRequestHandler((context, handlerToken) =>
        {
            Interlocked.Increment(ref countA);
            return repA.SendReplyAsync(context, ZMessage.FromOwned([.. "a"u8]), handlerToken);
        });
        repB.BindRequestHandler((context, handlerToken) =>
        {
            Interlocked.Increment(ref countB);
            return repB.SendReplyAsync(context, ZMessage.FromOwned([.. "b"u8]), handlerToken);
        });

        await using var req = new ZReqSocket();
        await req.ConnectAsync(endpointA, token);
        await req.ConnectAsync(endpointB, token);

        for (var i = 0; i < 8; i++)
        {
            var reply = await req.RequestAsync(ZMessage.FromOwned([.. "r"u8]), token);
            reply.Dispose();
        }

        Assert.Equal(4, countA);
        Assert.Equal(4, countB);
    }
}
