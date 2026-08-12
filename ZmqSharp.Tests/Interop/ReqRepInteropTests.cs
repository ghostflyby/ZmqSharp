using System.Buffers;
using System.Net.Sockets;
using System.Text;
using FluentAssertions;
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
    [Fact]
    public async Task ZmqSharpReq_NetMQRep_RoundTrips()
    {
        using var rep = new ResponseSocket();
        rep.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        rep.Bind($"tcp://127.0.0.1:{port}");

        await using var req = ZSocket.CreateReq();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await req.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        for (var i = 0; i < 5; i++)
        {
            // Our request is framed [empty, payload]; NetMQ REP strips the
            // delimiter and delivers the payload, echoing it re-framed.
            var pending = req.RequestAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"req-{i}")), cts.Token);

            var received = new NetMQMessage();
            rep.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref received).Should().BeTrue();
            received.Should().NotBeNull();
            received.FrameCount.Should().Be(1);
            received[0].ToByteArray().Should().Equal(Encoding.ASCII.GetBytes($"req-{i}"));
            rep.SendFrame(Encoding.ASCII.GetBytes($"ack-{i}"));

            var request = await pending;
            request[0].ToSequence().ToArray().Should().Equal(Encoding.ASCII.GetBytes($"ack-{i}"));
            request.Dispose();
        }
    }

    [Fact]
    public async Task NetMQReq_ZmqSharpRep_RoundTrips()
    {
        await using var rep = ZSocket.CreateRep();
        var port = InteropHelpers.GetFreePort();
        await rep.BindAsync($"tcp://127.0.0.1:{port}");
        rep.BindRequestHandler((context, token) =>
        {
            var payload = context[0].ToSequence().ToArray();
            return rep.SendReplyAsync(context, ZMessage.FromOwned(payload), token);
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

            reply.Should().Equal(Encoding.ASCII.GetBytes($"ping-{i}"));
        }
    }

    [Fact]
    public async Task ZmqSharpPair_NetMQDealer_HandshakeRejected()
    {
        using var dealer = new DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        await using var pair = ZSocket.CreatePairCallback();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // PAIR <-> DEALER is incompatible: establishment must fail. The
        // surfaced type is OS-dependent - the handshake rejection raises
        // ZeroMqProtocolException, but the peer's abortive close after our
        // ERROR can surface as IOException/SocketException on Windows and
        // Ubuntu (the documented teardown race in ZSocketBase).
        var failure = await Record.ExceptionAsync(() => pair.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token));
        failure.Should().NotBeNull();
        (failure is ZeroMqProtocolException or IOException or SocketException).Should().BeTrue();
    }
}
