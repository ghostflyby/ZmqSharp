using System.Buffers;
using System.Net.WebSockets;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// ZWS client integration (0035): the client connects to a real WebSocket
/// peer, negotiates the '+' subprotocol, completes the NULL READY exchange,
/// and maps one ZWS frame to one binary WebSocket message in both directions.
/// The test server is a thin HTTP upgrade over the BCL WebSocket server mode;
/// the library under test ships no server half.
/// </summary>
public sealed class ZwsClientTests
{
    [Fact(Timeout = 15_000)]
    public async Task Connect_OffersPlusSubprotocolAndExchangesFrames()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint("/chat"), token);
        await using var session = await accept;

        Assert.Contains("GET /chat HTTP/1.1", session.Request, StringComparison.Ordinal);
        Assert.Equal("ZWS2.0+NULL, ZWS2.0",
            ZwsTestServer.ReadHeader(session.Request, "Sec-WebSocket-Protocol"));
        Assert.DoesNotContain("ZWS2.0/NULL", session.Request, StringComparison.Ordinal);

        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildReady("PAIR"), token);
        var (readyFlag, readyBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x02, readyFlag);
        Assert.True(ZmtpCommandCodec.TryReadCommandName(readyBody, out var name));
        Assert.True(name.SequenceEqual("READY"u8));
        await connect;

        await pair.SendAsync("hello"u8.ToArray(), token);
        var (dataFlag, dataBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x00, dataFlag);
        Assert.Equal("hello"u8.ToArray(), dataBody);

        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "pong"u8.ToArray(), token);
        using var message = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal("pong"u8.ToArray(), message[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task Connect_NoMechanismFallbackExchangesIdentities()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        Assert.Equal("ZWS2.0+NULL, ZWS2.0",
            ZwsTestServer.ReadHeader(session.Request, "Sec-WebSocket-Protocol"));
        Assert.Equal(ZwsTestServer.NoMechanismSubProtocol, session.Socket.SubProtocol);

        // RFC 45 no mechanism: the first message each way is the routing
        // identity; there is no READY exchange.
        var (identityFlag, identityBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x00, identityFlag);
        Assert.Empty(identityBody);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, ReadOnlyMemory<byte>.Empty, token);
        await connect;

        await pair.SendAsync("no-mechanism"u8.ToArray(), token);
        var (dataFlag, dataBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x00, dataFlag);
        Assert.Equal("no-mechanism"u8.ToArray(), dataBody);

        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "reply"u8.ToArray(), token);
        using var message = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal("reply"u8.ToArray(), message[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_AdvertisesTheConfiguredRoutingIdentity()
    {
        await using var server = ZwsTestServer.Start();
        await using var dealer = new ZDealerSocket(
            new ZSocketOptions { Identity = "dealer-1"u8.ToArray() });
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = dealer.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (flag, body) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x00, flag);
        Assert.Equal("dealer-1"u8.ToArray(), body);

        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "server-1"u8.ToArray(), token);
        await connect;
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_PeerIdentityIsRegisteredByRouter()
    {
        await using var server = ZwsTestServer.Start();
        await using var router = new ZRouterSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = router.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (clientFlag, clientIdentity) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x00, clientFlag);
        Assert.Empty(clientIdentity);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "peer-1"u8.ToArray(), token);
        await connect;

        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "hello"u8.ToArray(), token);
        using var message = await ReadMessageAsync(router.Messages, token);
        Assert.Equal(2, message.Count);
        Assert.Equal("peer-1"u8.ToArray(), message[0].ToSequence().ToArray());
        Assert.Equal("hello"u8.ToArray(), message[1].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_MultipartIdentity_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (_, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "b"u8.ToArray(), token);

        var error = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => connect);
        Assert.Contains("identity must be a single frame", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_ThirdIdentityFrame_IsRejected()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (_, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "b"u8.ToArray(), token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "c"u8.ToArray(), token);

        var rejection = await Assert.ThrowsAsync<ZReceiveRejectedException>(() => connect);
        Assert.Equal(ZReceiveRejectionReason.TooManyFrames, rejection.Rejection.Reason);
        Assert.Equal(2, rejection.Rejection.Limit);
        Assert.Equal(3, rejection.Rejection.Actual);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_IsRejectedForANonNullSocket()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("user", "pass"u8) },
        });
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => connect);
        Assert.Contains("no-mechanism profile", error.Message);
    }

    [Fact(Timeout = 15_000)]
    public async Task NoMechanism_CommandInsteadOfIdentity_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(
            selectedProtocol: ZwsTestServer.NoMechanismSubProtocol, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (_, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildReady("PAIR"), token);

        var error = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => connect);
        Assert.Contains("identity message, not a command", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task MultipartMessages_AreAggregatedAndSentPerFrame()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "b"u8.ToArray(), token);
        using var incoming = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal(2, incoming.Count);
        Assert.Equal("a"u8.ToArray(), incoming[0].ToSequence().ToArray());
        Assert.Equal("b"u8.ToArray(), incoming[1].ToSequence().ToArray());

        await pair.SendAsync(new[] { "x"u8.ToArray(), "y"u8.ToArray() }, token);
        var (firstFlag, firstBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        var (secondFlag, secondBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x01, firstFlag);
        Assert.Equal("x"u8.ToArray(), firstBody);
        Assert.Equal(0x00, secondFlag);
        Assert.Equal("y"u8.ToArray(), secondBody);
    }

    [Fact(Timeout = 15_000)]
    public async Task LargeMessage_IsReceivedIntact()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var payload = new byte[10 * 1024];
        Random.Shared.NextBytes(payload);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, payload, token);

        using var message = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal(payload, message[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task FragmentedWebSocketMessage_IsReassembled()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var payload = "fragmented-payload"u8.ToArray();
        await ZwsTestWire.SendFragmentedFrameAsync(session.Socket, 0x00, payload, token);

        using var message = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal(payload, message[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task OversizedFrame_IsRejectedBeforeTheWholeMessageIsBuffered()
    {
        await using var server = ZwsTestServer.Start();
        using var pool = new ProbingMemoryPool();
        await using var pair = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            MaxFrameLength = 64,
        });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, new byte[64 * 1024], token);
        var failure = await ended.WaitAsync(token);
        var rejection = Assert.IsType<ZReceiveRejectedException>(failure);
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, rejection.Rejection.Reason);
        Assert.Equal(64, rejection.Rejection.Limit);
        Assert.True(rejection.Rejection.Actual is > 64);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.MessageTooBig,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));

        // The read stopped at the first 4 KiB chunk instead of growing the
        // buffer to the peer's message size (the frame bound was checked
        // before allocation).
        Assert.True(pool.MaxRequested <= 4096, $"receive path rented {pool.MaxRequested} bytes");
    }

    [Fact(Timeout = 15_000)]
    public async Task FrameAtExactBoundary_IsAccepted()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions { MaxFrameLength = 5 });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var payload = "abcde"u8.ToArray();
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, payload, token);

        using var message = await ReadMessageAsync(pair.Messages, token);
        Assert.Equal(payload, message[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 15_000)]
    public async Task FragmentedFrameOnePastBoundary_IsRejected()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions { MaxFrameLength = 5 });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFragmentedFrameAsync(
            session.Socket, 0x00, "abcdef"u8.ToArray(), token);

        var failure = await ended.WaitAsync(token);
        var rejection = Assert.IsType<ZReceiveRejectedException>(failure);
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, rejection.Rejection.Reason);
        Assert.Equal(5, rejection.Rejection.Limit);
        Assert.Equal(6, rejection.Rejection.Actual);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.MessageTooBig,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task OversizedCommand_IsRejectedBeforeTheWholeMessageIsBuffered()
    {
        await using var server = ZwsTestServer.Start();
        using var pool = new ProbingMemoryPool();
        await using var pair = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            MaxCommandSize = 256,
        });
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (readyFlag, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x02, readyFlag);
        await ZwsTestWire.SendCommandAsync(session.Socket, new byte[64 * 1024], token);

        var rejection = await Assert.ThrowsAsync<ZReceiveRejectedException>(() => connect);
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, rejection.Rejection.Reason);
        Assert.Equal(256, rejection.Rejection.Limit);
        Assert.True(rejection.Rejection.Actual is > 256);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.MessageTooBig,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
        Assert.True(pool.MaxRequested <= 4096, $"handshake rented {pool.MaxRequested} bytes");
    }

    [Fact(Timeout = 15_000)]
    public async Task OversizedMessage_IsRejectedBeforeDelivery()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions { MaxMessageLength = 150 });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, new byte[100], token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, new byte[100], token);

        var failure = await ended.WaitAsync(token);
        var rejection = Assert.IsType<ZReceiveRejectedException>(failure);
        Assert.Equal(ZReceiveRejectionReason.MessageTooLarge, rejection.Rejection.Reason);
        Assert.Equal(150, rejection.Rejection.Limit);
        Assert.Equal(200, rejection.Rejection.Actual);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.MessageTooBig,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task OversizedSingleFrameMessage_IsRejectedBeforeBuffering()
    {
        await using var server = ZwsTestServer.Start();
        using var pool = new ProbingMemoryPool();
        await using var pair = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            MaxMessageLength = 150,
        });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, new byte[64 * 1024], token);
        var failure = await ended.WaitAsync(token);
        var rejection = Assert.IsType<ZReceiveRejectedException>(failure);
        Assert.Equal(ZReceiveRejectionReason.MessageTooLarge, rejection.Rejection.Reason);
        Assert.Equal(150, rejection.Rejection.Limit);
        Assert.True(rejection.Rejection.Actual is > 150);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.MessageTooBig,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
        Assert.True(pool.MaxRequested <= 4096, $"receive path rented {pool.MaxRequested} bytes");
    }

    [Fact(Timeout = 15_000)]
    public async Task ExcessiveFrameCount_IsRejectedBeforeDelivery()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions { MaxFramesPerMessage = 1 });
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x00, "b"u8.ToArray(), token);

        var failure = await ended.WaitAsync(token);
        var rejection = Assert.IsType<ZReceiveRejectedException>(failure);
        Assert.Equal(ZReceiveRejectionReason.TooManyFrames, rejection.Rejection.Reason);
        Assert.Equal(1, rejection.Rejection.Limit);
        Assert.Equal(2, rejection.Rejection.Actual);
        Assert.Equal(1, pair.ReceiveRejections);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Theory(Timeout = 15_000)]
    [InlineData((byte)0x03, "command frame cannot carry the MORE flag")]
    [InlineData((byte)0x04, "reserved ZWS frame flag bits are set")]
    [InlineData((byte)0x80, "reserved ZWS frame flag bits are set")]
    public async Task InvalidFrameFlags_FaultThePeer(byte flag, string expected)
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, flag, "x"u8.ToArray(), token);
        var failure = await ended.WaitAsync(token);
        var error = Assert.IsType<ZeroMqProtocolException>(failure);
        Assert.Contains(expected, error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task TextMessage_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await session.Socket.SendAsync("text"u8.ToArray(), WebSocketMessageType.Text, true, token);
        var failure = await ended.WaitAsync(token);
        var error = Assert.IsType<ZeroMqProtocolException>(failure);
        Assert.Contains("binary", error.Message);

        // RFC 6455 7.1.7: a protocol failure closes with an appropriate status.
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task EmptyMessage_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await session.Socket.SendAsync(Array.Empty<byte>(), WebSocketMessageType.Binary, true, token);
        var failure = await ended.WaitAsync(token);
        var error = Assert.IsType<ZeroMqProtocolException>(failure);
        Assert.Contains("empty", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task ErrorCommand_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildError("boom"), token);
        var failure = await ended.WaitAsync(token);
        var error = Assert.IsType<ZeroMqProtocolException>(failure);
        Assert.Contains("peer sent ERROR", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task NullReadyError_FailsConnectWithProtocolClose()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var (readyFlag, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x02, readyFlag);
        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildError("boom"), token);

        var error = await Assert.ThrowsAsync<ZMechanismException>(() => connect);
        Assert.Contains("peer sent ERROR", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task PeerClose_EndsThePeerCleanly()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await session.Socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "done", token);
        var failure = await ended.WaitAsync(token);
        Assert.Null(failure);

        // RFC 6455 5.5.1: the endpoint that received Close replies with Close.
        Assert.Equal(
            WebSocketCloseStatus.NormalClosure,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task Handshake_ReturnsEveryPooledReceiveBuffer()
    {
        await using var server = ZwsTestServer.Start();
        var pool = new CountingMemoryPool();
        var token = TestContext.Current.CancellationToken;

        await using (var pair = new ZPairSocket(new ZSocketOptions { Pool = pool }))
        {
            var session = await ConnectAsync(server, pair, token);
            await session.DisposeAsync();
        }

        // The mechanism context owns the final peer command; if it is
        // released before the READY exchange finishes, that message's pooled
        // receive buffer is stranded.
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
        Assert.Equal(0, pool.Outstanding);
    }

    [Fact(Timeout = 15_000)]
    public async Task EofInTheMiddleOfAMoreChain_EndsThePeerCleanly()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await session.Socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "done", token);

        var failure = await ended.WaitAsync(token);
        Assert.Null(failure);
    }

    [Fact(Timeout = 15_000)]
    public async Task CommandInterruptingAMoreChain_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await ZwsTestWire.SendFrameAsync(session.Socket, 0x01, "a"u8.ToArray(), token);
        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildReady("PAIR"), token);

        var failure = await ended.WaitAsync(token);
        var error = Assert.IsType<ZeroMqProtocolException>(failure);
        Assert.Contains("command frame interrupts a More chain", error.Message);
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task ServerOmittingTheSubprotocol_FailsConnect()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(selectedProtocol: null, token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var error = await Assert.ThrowsAsync<WebSocketException>(() => connect);
        Assert.Contains("ZWS requires", error.Message);
    }

    [Fact(Timeout = 15_000)]
    public async Task DisconnectByAddress_StopsTheWsPeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await using var session = await ConnectAsync(server, pair, token);

        var ended = PeerEndedAsync(pair);
        await pair.DisconnectAsync(server.Endpoint(), token);
        var failure = await ended.WaitAsync(token);
        Assert.Null(failure);
    }

    [Fact(Timeout = 15_000)]
    public async Task SocketTypeMismatch_FaultsThePeer()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildReady("PULL"), token);
        var (readyFlag, _) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x02, readyFlag);

        var error = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => connect);
        Assert.Contains("is not accepted by local socket type 'PAIR'", error.Message);

        // The runtime answers the peer's invalid READY with an ERROR command.
        var (errorFlag, errorBody) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
        Assert.Equal(0x02, errorFlag);
        Assert.True(ZmtpCommandCodec.TryReadCommandName(errorBody, out var name));
        Assert.True(name.SequenceEqual("ERROR"u8));
        Assert.Equal(
            WebSocketCloseStatus.ProtocolError,
            await ZwsTestWire.ReceiveCloseStatusAsync(session.Socket, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task PlainMechanism_IsRejectedOverZws()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("user", "pass"u8) },
        });
        var token = TestContext.Current.CancellationToken;

        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => connect);
        Assert.Contains("negotiates the 'NULL' mechanism", error.Message);
    }

    [Fact(Timeout = 15_000)]
    public async Task ServerSelectingAnUnofferedSubprotocol_FailsConnect()
    {
        await using var server = ZwsTestServer.Start();
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        // "chat" is a legal token but was not offered; the BCL client must
        // reject the server's selection.
        var accept = server.AcceptAsync("chat", token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        await using var session = await accept;

        await Assert.ThrowsAsync<WebSocketException>(() => connect);
    }

    [Fact]
    public async Task Bind_IsRejectedForWebSocketEndpoints()
    {
        await using var pair = new ZPairSocket();
        var error = await Assert.ThrowsAsync<NotSupportedException>(
            () => pair.BindAsync("ws://127.0.0.1:1/", TestContext.Current.CancellationToken));
        Assert.Contains("client-only", error.Message);
    }

    [Fact]
    public async Task EndpointParsing_MapsWsSchemesToTheWebSocketTransport()
    {
        var parsed = await ZEndpointParser.ParseEndpointAsync("ws://127.0.0.1:9000/chat", CancellationToken.None);
        var endpoint = Assert.IsType<WebSocketEndpoint>(parsed);
        Assert.Equal("127.0.0.1", endpoint.Host);
        Assert.Equal(9000, endpoint.Port);
        Assert.Equal("/chat", endpoint.Path);
        Assert.False(endpoint.Secure);

        var secure = Assert.IsType<WebSocketEndpoint>(
            await ZEndpointParser.ParseEndpointAsync("wss://example.com", CancellationToken.None));
        Assert.Equal(443, secure.Port);
        Assert.True(secure.Secure);
        Assert.Equal("/", secure.Path);

        var ipv6 = Assert.IsType<WebSocketEndpoint>(
            await ZEndpointParser.ParseEndpointAsync("ws://[::1]:8080/x", CancellationToken.None));
        Assert.Equal("[::1]", ipv6.Host);

        Assert.Equal(typeof(WebSocketTransport), ZEndpointParser.TransportTypeOf("ws://a/"));
        Assert.Equal(typeof(WebSocketTransport), ZEndpointParser.TransportTypeOf("wss://a/"));
        Assert.Equal(typeof(SocketTransport), ZEndpointParser.TransportTypeOf("tcp://a:1"));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => ZEndpointParser.ParseEndpointAsync("udp://a:1", CancellationToken.None));
    }

    [Fact]
    public void Endpoint_RoundTripsAndCompares()
    {
        var endpoint = new WebSocketEndpoint("server.example.com", 9000, "/chat", secure: false);
        Assert.Equal("ws://server.example.com:9000/chat", endpoint.ToString());
        Assert.Equal(endpoint, new WebSocketEndpoint("SERVER.example.com", 9000, "/chat", secure: false));
        Assert.NotEqual(endpoint, new WebSocketEndpoint("server.example.com", 9000, "/chat", secure: true));
        Assert.NotEqual(endpoint, new WebSocketEndpoint("server.example.com", 9001, "/chat", secure: false));
        Assert.Equal("/", new WebSocketEndpoint("h", 1, "", false).Path);

        // The URL form follows the BCL WebSocket/Uri rules: default ports are
        // omitted, IPv6 literals are bracketed, a missing leading slash is
        // added, and query strings survive.
        Assert.Equal("ws://h/", new WebSocketEndpoint("h", 80, "", false).ToString());
        Assert.Equal("wss://h/", new WebSocketEndpoint("h", 443, "", true).ToString());
        Assert.Equal("ws://[::1]:9000/x",
            new WebSocketEndpoint("::1", 9000, "/x", false).ToString());
        Assert.Equal("/x", new WebSocketEndpoint("h", 1, "x", false).Path);
        Assert.Equal("ws://h:1/x?a=b",
            new WebSocketEndpoint("h", 1, "/x?a=b", false).ToString());
        Assert.Equal("ws://h:1/x",
            new WebSocketEndpoint("h", 1, "/x#fragment", false).ToString());
        Assert.Equal("/a%20b", new WebSocketEndpoint("h", 1, "/a b", false).Path);
        Assert.Equal("/a%2Fb", new WebSocketEndpoint("h", 1, "/a%2Fb", false).Path);
        Assert.Equal("ws://h:1/a%20b",
            new WebSocketEndpoint("h", 1, "/a b", false).ToString());
        Assert.Equal("ws://h:1/x?a=b",
            new WebSocketEndpoint("h", 1, "/x?a=b#c", false).ToString());

        Assert.Throws<ArgumentException>(() => new WebSocketEndpoint("bad/host", 1, "/", false));
        Assert.Throws<ArgumentOutOfRangeException>(() => new WebSocketEndpoint("h", -1, "/", false));
        Assert.Throws<ArgumentOutOfRangeException>(() => new WebSocketEndpoint("h", 70_000, "/", false));
    }

    [Fact]
    public async Task Teardown_UnknownScheme_IsANoOp()
    {
        await using var pair = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await pair.DisconnectAsync("udp://a:1", token);
        await pair.UnbindAsync("udp://a:1", token);
    }

    /// <summary>Runs the server's READY first, then the client's, and completes ConnectAsync.</summary>
    private static async Task<ZwsTestSession> ConnectAsync(
        ZwsTestServer server, ZPairSocket pair, CancellationToken token)
    {
        var accept = server.AcceptAsync(token: token);
        var connect = pair.ConnectAsync(server.Endpoint(), token);
        var session = await accept;
        try
        {
            await ZwsTestWire.SendCommandAsync(session.Socket, ZmtpCommands.BuildReady("PAIR"), token);
            var (flag, body) = await ZwsTestWire.ReceiveFrameAsync(session.Socket, token);
            Assert.Equal(0x02, flag);
            Assert.True(ZmtpCommandCodec.TryReadCommandName(body, out var name));
            Assert.True(name.SequenceEqual("READY"u8));
            await connect;
            return session;
        }
        catch
        {
            await session.DisposeAsync();
            throw;
        }
    }

    private static Task<Exception?> PeerEndedAsync(ZPairSocket pair)
    {
        var ended = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        pair.PeerEnded += (_, failure) => ended.TrySetResult(failure);
        return ended.Task;
    }

    private static async Task<ZMessage> ReadMessageAsync(
        ChannelReader<ZMessage> reader, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        return await reader.ReadAsync(timeout.Token);
    }
}
