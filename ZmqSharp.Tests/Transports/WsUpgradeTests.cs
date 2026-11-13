using System.Text;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// Byte-level vectors for the ZWS HTTP upgrade (0033): the request parser is
/// locked against libzmq's ws_engine request shape, and the accept-key
/// computation against RFC 6455 section 1.3's worked example. pyzmq's bundled
/// libzmq (4.3.5) ships without the ws transport, so these vectors carry the
/// wire compatibility burden instead of an interop peer.
/// </summary>
public sealed class WsUpgradeTests
{
    private const string SampleKey = "dGhlIHNhbXBsZSBub25jZQ==";

    // RFC 6455 section 1.3: this key must produce exactly this accept token.
    private const string SampleAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

    /// <summary>The upgrade request libzmq's ws_engine sends for NULL security.</summary>
    private const string LibzmqRequest =
        "GET /chat HTTP/1.1\r\n" +
        "Host: server.example.com:9000\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        $"Sec-WebSocket-Key: {SampleKey}\r\n" +
        "Sec-WebSocket-Protocol: ZWS2.0/NULL,ZWS2.0\r\n" +
        "Sec-WebSocket-Version: 13\r\n" +
        "\r\n";

    private static byte[] Request(string header) => [.. Encoding.ASCII.GetBytes(header)];

    [Fact]
    public void ComputeAcceptKey_MatchesRfc6455Example()
    {
        Assert.Equal(SampleAccept, WsUpgrade.ComputeAcceptKey(SampleKey));
    }

    [Fact]
    public void TryParseRequest_AcceptsTheLibzmqUpgrade()
    {
        Assert.True(WsUpgrade.TryParseRequest(
            Encoding.ASCII.GetString(Request(LibzmqRequest)),
            out var request));

        Assert.Equal("/chat", request.Path);
        Assert.Equal(SampleKey, request.SecWebSocketKey);
        Assert.Equal(["ZWS2.0/NULL", "ZWS2.0"], request.Protocols);
    }

    [Theory]
    [InlineData("POST / HTTP/1.1")]
    [InlineData("GET / HTTP/1.0")]
    [InlineData("GET  HTTP/1.1")]
    public void Rejects_NonUpgradeRequestLines(string requestLine)
    {
        var header = requestLine + "\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                     $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
                     "Sec-WebSocket-Version: 13\r\n\r\n";

        Assert.False(WsUpgrade.TryParseRequest(header, out _));
    }

    [Theory]
    [InlineData("websocketx")]
    [InlineData("")]
    public void Rejects_WrongUpgradeToken(string upgrade)
    {
        var header = "GET / HTTP/1.1\r\nUpgrade: " + upgrade + "\r\nConnection: Upgrade\r\n" +
                     $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
                     "Sec-WebSocket-Version: 13\r\n\r\n";

        Assert.False(WsUpgrade.TryParseRequest(header, out _));
    }

    [Fact]
    public void Rejects_ConnectionWithoutUpgradeToken()
    {
        var header = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: keep-alive\r\n" +
                     $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
                     "Sec-WebSocket-Version: 13\r\n\r\n";

        Assert.False(WsUpgrade.TryParseRequest(header, out _));
    }

    [Fact]
    public void Accepts_ConnectionWithMultipleTokens()
    {
        var header = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n" +
                     $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
                     "Sec-WebSocket-Version: 13\r\n\r\n";

        Assert.True(WsUpgrade.TryParseRequest(header, out _));
    }

    [Fact]
    public void Rejects_UnsupportedVersionAndMissingKeyOrProtocol()
    {
        var version12 = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                        $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
                        "Sec-WebSocket-Version: 12\r\n\r\n";
        Assert.False(WsUpgrade.TryParseRequest(version12, out _));

        var noKey = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                    "Sec-WebSocket-Protocol: ZWS2.0/NULL\r\nSec-WebSocket-Version: 13\r\n\r\n";
        Assert.False(WsUpgrade.TryParseRequest(noKey, out _));

        var noProtocol = "GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                         $"Sec-WebSocket-Key: {SampleKey}\r\nSec-WebSocket-Version: 13\r\n\r\n";
        Assert.False(WsUpgrade.TryParseRequest(noProtocol, out _));
    }

    [Fact]
    public void SwitchingProtocols_ResponseBytesMatchLibzmqShape()
    {
        var response = Encoding.ASCII.GetString(WsUpgrade.BuildSwitchingProtocols(SampleAccept));

        Assert.Equal(
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Accept: {SampleAccept}\r\n" +
            "Sec-WebSocket-Protocol: ZWS2.0/NULL\r\n" +
            "\r\n",
            response);
    }

    [Fact]
    public void BadRequest_ResponseIsTerminalClose()
    {
        Assert.Equal(
            "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            Encoding.ASCII.GetString(WsUpgrade.BuildBadRequest()));
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
}
