using System.Buffers;
using System.Net.Sockets;
using System.Text;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
///     REQ/REP interop with the NetMQ libzmq-compatible implementation over TCP
///     (0006 section 5): the empty-delimiter wire framing is exercised in both
///     directions, plus an incompatible Socket-Type pairing is rejected.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class ReqRepInteropTests
{
    [Fact(Timeout = 20_000)]
    public async Task ZmqSharpReq_NetMQRep_RoundTrips()
    {
        using var rep = new ResponseSocket();
        rep.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        rep.Bind($"tcp://127.0.0.1:{port}");

        await using var req = new ZReqSocket();
        var token = TestContext.Current.CancellationToken;
        await req.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        for (var i = 0; i < 5; i++)
        {
            // Our request is framed [empty, payload]; NetMQ REP strips the
            // delimiter and delivers the payload, echoing it re-framed.
            var pending = req.RequestAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"req-{i}")), token);

            var received = new NetMQMessage();
            Assert.True(rep.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref received));
            Assert.NotNull(received);
            Assert.Equal(1, received.FrameCount);
            Assert.Equal(Encoding.ASCII.GetBytes($"req-{i}"), received[0].ToByteArray());
            rep.SendFrame(Encoding.ASCII.GetBytes($"ack-{i}"));

            var request = await pending;
            Assert.Equal(Encoding.ASCII.GetBytes($"ack-{i}"), request[0].ToSequence().ToArray());
            request.Dispose();
        }
    }

    [Fact]
    public async Task NetMQReq_ZmqSharpRep_RoundTrips()
    {
        await using var rep = new ZRepSocket();
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await rep.BindAsync($"tcp://127.0.0.1:{port}", token);
        rep.BindRequestHandler((context, replyToken) =>
        {
            var payload = context[0].ToSequence().ToArray();
            return rep.SendReplyAsync(context, ZMessage.FromOwned(payload), replyToken);
        });

        using var req = new RequestSocket();
        req.Options.Linger = TimeSpan.Zero;
        req.Connect($"tcp://127.0.0.1:{port}");

        for (var i = 0; i < 5; i++)
        {
            // NetMQ REQ frames [request, empty]; our REP strips it and the
            // handler echoes, re-framing [reply, empty] back.
            req.SendFrame(Encoding.ASCII.GetBytes($"ping-{i}"));
            if (!req.TryReceiveFrameBytes(TimeSpan.FromSeconds(5), out var reply))
                throw new TimeoutException("expected a reply within the timeout");

            Assert.Equal(Encoding.ASCII.GetBytes($"ping-{i}"), reply);
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task ZmqSharpPair_NetMQDealer_HandshakeRejected()
    {
        using var dealer = new DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        // PAIR <-> DEALER is incompatible: establishment must fail. The
        // surfaced type is OS-dependent - the handshake rejection raises
        // ZeroMqProtocolException, but the peer's abortive close after our
        // ERROR can surface as IOException/SocketException on Windows and
        // Ubuntu (the documented teardown race in ZSocketBase).
        var failure = await Record.ExceptionAsync(() => pair.ConnectAsync($"tcp://127.0.0.1:{port}", token));
        Assert.NotNull(failure);
        Assert.True(failure is ZeroMqProtocolException or IOException or SocketException);
    }
}
