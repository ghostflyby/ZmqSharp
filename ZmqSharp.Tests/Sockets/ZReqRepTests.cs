using System.Buffers;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
///     REQ/REP pattern tests (0010): strict alternation, round-robin,
///     delimiter framing, directed replies, and peer-retirement behavior.
/// </summary>
public sealed class ZReqRepTests
{
    [Fact]
    public async Task ReqRep_RoundTripsRequestAndReply()
    {
        await using var rep = ZSocket.CreateRep();
        var port = GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        rep.BindRequestHandler(async (context, token) =>
        {
            context.Should().HaveCount(1);
            var payload = context[0].ToSequence().ToArray();
            await rep.SendReplyAsync(context, ZMessage.FromOwned(payload), token);
        });

        await using var req = ZSocket.CreateReq();
        await req.ConnectAsync($"tcp://127.0.0.1:{port}");
        var reply = await req.RequestAsync(ZMessage.FromOwned([.. "ping"u8]));

        reply.Should().HaveCount(1);
        reply[0].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        reply.Dispose();
    }

    [Fact]
    public async Task ReqRep_MultipartRequestAndReply()
    {
        await using var rep = ZSocket.CreateRep();
        var port = GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        rep.BindRequestHandler((context, token) =>
        {
            context.Should().HaveCount(2);
            return rep.SendReplyAsync(context, MessageFactory.Multipart([.. "x"u8], [.. "y"u8]), token);
        });

        await using var req = ZSocket.CreateReq();
        await req.ConnectAsync($"tcp://127.0.0.1:{port}");
        var reply = await req.RequestAsync(MessageFactory.Multipart([.. "a"u8], [.. "b"u8]));

        reply.Count.Should().Be(2);
        reply[0].ToSequence().ToArray().Should().Equal([.. "x"u8]);
        reply[1].ToSequence().ToArray().Should().Equal([.. "y"u8]);
        reply.Dispose();
    }

    [Fact]
    public async Task Req_StrictAlternation_ThrowsWhileInFlight()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rep = ZSocket.CreateRep();
        var port = GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        rep.BindRequestHandler(async (context, token) =>
        {
            await release.Task.WaitAsync(token);
            await rep.SendReplyAsync(context, ZMessage.FromOwned([.. "ok"u8]), token);
        });

        await using var req = ZSocket.CreateReq();
        await req.ConnectAsync($"tcp://127.0.0.1:{port}");

        var first = req.RequestAsync(ZMessage.FromOwned([.. "1"u8]));

        Func<Task> act = () => req.RequestAsync(ZMessage.FromOwned([.. "2"u8]));
        await act.Should().ThrowAsync<InvalidOperationException>();

        release.TrySetResult();
        var reply = await first;
        reply.Dispose();

        // After the reply lands the gate reopens: a second request succeeds.
        var second = await req.RequestAsync(ZMessage.FromOwned([.. "3"u8]));
        second.Dispose();
    }

    [Fact]
    public async Task Req_PeerClosesBeforeReply_FaultsRequestAndFreesGate()
    {
        await using var rep = ZSocket.CreateRep();
        var port = GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        // No handler bound: requests arrive and are dropped, so the reply
        // never comes and the peer stays alive until we close it.

        await using var req = ZSocket.CreateReq();
        await req.ConnectAsync($"tcp://127.0.0.1:{port}");
        var requestTask = req.RequestAsync(ZMessage.FromOwned([.. "1"u8]));

        // Closing the REP peer retires the REQ's current connection and
        // faults the in-flight request (0010 section 2).
        await rep.DisposeAsync();

        var ex = await Record.ExceptionAsync(() => requestTask.WaitAsync(TimeSpan.FromSeconds(5)));
        ex.Should().NotBeNull();
        (ex is IOException or SocketException or ObjectDisposedException).Should().BeTrue();
    }

    [Fact]
    public async Task Req_NoPeer_Throws()
    {
        await using var req = ZSocket.CreateReq();
        Func<Task> act = () => req.RequestAsync(ZMessage.FromOwned([.. "x"u8]));
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Rep_RoutesReplyToOriginatingPeer_WithTwoPeers()
    {
        await using var rep = ZSocket.CreateRep();
        var port = GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        rep.BindRequestHandler(async (context, token) =>
        {
            var payload = context[0].ToSequence().ToArray();
            await rep.SendReplyAsync(context, ZMessage.FromOwned(payload), token);
        });

        await using var reqA = ZSocket.CreateReq();
        await using var reqB = ZSocket.CreateReq();
        await reqA.ConnectAsync($"tcp://127.0.0.1:{port}");
        await reqB.ConnectAsync($"tcp://127.0.0.1:{port}");

        var replyA = await reqA.RequestAsync(ZMessage.FromOwned([.. "a"u8]));
        replyA[0].ToSequence().ToArray().Should().Equal([.. "a"u8]);
        replyA.Dispose();

        var replyB = await reqB.RequestAsync(ZMessage.FromOwned([.. "b"u8]));
        replyB[0].ToSequence().ToArray().Should().Equal([.. "b"u8]);
        replyB.Dispose();
    }

    [Fact]
    public async Task Req_RoundRobinsAcrossTwoPeers()
    {
        await using var repA = ZSocket.CreateRep();
        await using var repB = ZSocket.CreateRep();
        var portA = GetFreePort();
        var portB = GetFreePort();
        await repA.BindAsync($"tcp://127.0.0.1:{portA}");
        await repB.BindAsync($"tcp://127.0.0.1:{portB}");
        var countA = 0;
        var countB = 0;
        repA.BindRequestHandler((context, token) =>
        {
            Interlocked.Increment(ref countA);
            return repA.SendReplyAsync(context, ZMessage.FromOwned([.. "a"u8]), token);
        });
        repB.BindRequestHandler((context, token) =>
        {
            Interlocked.Increment(ref countB);
            return repB.SendReplyAsync(context, ZMessage.FromOwned([.. "b"u8]), token);
        });

        await using var req = ZSocket.CreateReq();
        await req.ConnectAsync($"tcp://127.0.0.1:{portA}");
        await req.ConnectAsync($"tcp://127.0.0.1:{portB}");

        for (var i = 0; i < 8; i++)
        {
            var reply = await req.RequestAsync(ZMessage.FromOwned([.. "r"u8]));
            reply.Dispose();
        }

        countA.Should().Be(4);
        countB.Should().Be(4);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
