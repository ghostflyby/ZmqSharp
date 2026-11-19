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
        Assert.Equal("ZWS2.0+NULL",
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
