using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// ZWS transport integration (0033): a raw TCP client performs the HTTP
/// upgrade by hand and drives the ZWS wire protocol (command/data messages
/// with the ZMTP flag byte) against a bound ZmqSharp socket, locking the
/// upgrade response, the NULL READY exchange, and the frame mapping at byte
/// level. pyzmq's libzmq (4.3.5) ships without ws, so this suite is the
/// libzmq-interop stand-in.
/// </summary>
public sealed class ZSocketWebSocketTests
{
    private const string UpgradeKey = "dGhlIHNhbXBsZSBub25jZQ==";

    [Fact(Timeout = 15_000)]
    public async Task RawClient_CompletesHandshakeAndExchangesFrames()
    {
        var port = GetFreePort();
        await using var server = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"ws://127.0.0.1:{port}/", token);

        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, token);
        var stream = tcp.GetStream();

        // The upgrade request in libzmq ws_engine's shape (NULL offers first).
        var request =
            "GET / HTTP/1.1\r\n" +
            $"Host: 127.0.0.1:{port}\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Key: {UpgradeKey}\r\n" +
            "Sec-WebSocket-Protocol: ZWS2.0/NULL,ZWS2.0\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request), token);

        var (response, leftover) = await ReadHttpHeaderAsync(stream, token);
        Assert.StartsWith("HTTP/1.1 101", response, StringComparison.Ordinal);
        Assert.Contains($"Sec-WebSocket-Accept: {WsUpgrade.ComputeAcceptKey(UpgradeKey)}", response);
        Assert.Contains("Sec-WebSocket-Protocol: ZWS2.0/NULL", response);

        // The header read may overshoot into the server's first pipelined
        // frame (its READY); replay the leftover before the raw stream. The
        // BCL client side applies RFC 6455 masking, like libzmq's.
        using var ws = WebSocket.CreateFromStream(new PrefixedStream(leftover, stream), isServer: false, null,
            TimeSpan.Zero);

        // The server's READY arrives as one command message: flag 0x02 +
        // the standard ZMTP READY body (no greeting, 0033).
        var ready = await ReceiveMessageAsync(ws, token);
        Assert.Equal(0x02, ready[0]);
        Assert.True(ZmtpCommandCodec.TryReadCommandName(ready.AsSpan(1), out var name));
        Assert.True(name.SequenceEqual("READY"u8));
        Assert.Equal("PAIR", ZmtpCommandCodec.ParseReadySocketType(ready.AsSpan(2 + name.Length)));

        // The client READY completes the NULL handshake.
        await SendZwsFrameAsync(ws, [0x02, .. ZmtpCommands.BuildReady("PAIR")], token);

        // Data frames: one binary message per ZMTP frame, flag byte first.
        await SendZwsFrameAsync(ws, [0x00, .. "hello"u8], token);
        var message = await ReadMessageAsync(server.Messages, token);
        Assert.NotNull(message);
        Assert.Equal([.. "hello"u8], message.Value[0].ToSequence().ToArray());

        // Multipart: MORE flag on all but the last frame.
        await SendZwsFrameAsync(ws, [0x01, .. "a"u8], token);
        await SendZwsFrameAsync(ws, [0x00, .. "b"u8], token);
        var multipart = await ReadMessageAsync(server.Messages, token);
        Assert.NotNull(multipart);
        Assert.Equal(2, multipart.Value.Count);
        Assert.Equal([.. "a"u8], multipart.Value[0].ToSequence().ToArray());
        Assert.Equal([.. "b"u8], multipart.Value[1].ToSequence().ToArray());

        // Server-to-client frames are unmasked binary messages.
        await server.SendAsync(ZMessage.FromOwned([.. "pong"u8]), token);
        var pong = await ReceiveMessageAsync(ws, token);
        Assert.Equal([0x00, .. "pong"u8], pong);

        message?.Dispose();
        multipart?.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task PlainHttpRequest_IsRejectedWithBadRequest()
    {
        var port = GetFreePort();
        await using var server = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"ws://127.0.0.1:{port}/", token);

        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, token);
        var stream = tcp.GetStream();
        await stream.WriteAsync(
            "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"u8.ToArray(), token);

        var (response, _) = await ReadHttpHeaderAsync(stream, token);
        Assert.StartsWith("HTTP/1.1 400", response, StringComparison.Ordinal);
        // Terminal: the server closes the connection after the rejection.
        var tail = new byte[1];
        Assert.Equal(0, await stream.ReadAsync(tail, token));
    }

    [Fact(Timeout = 15_000)]
    public async Task UpgradeWithoutZwsSubprotocol_IsRejectedWithBadRequest()
    {
        var port = GetFreePort();
        await using var server = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"ws://127.0.0.1:{port}/", token);

        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, token);
        var stream = tcp.GetStream();
        var request =
            "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
            $"Sec-WebSocket-Key: {UpgradeKey}\r\n" +
            "Sec-WebSocket-Protocol: chat, echo\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request), token);

        var (response, _) = await ReadHttpHeaderAsync(stream, token);
        Assert.StartsWith("HTTP/1.1 400", response, StringComparison.Ordinal);
    }

    [Fact(Timeout = 15_000)]
    public async Task SocketTypeMismatch_FaultsWithProtocolError()
    {
        var port = GetFreePort();
        await using var server = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"ws://127.0.0.1:{port}/", token);

        // PULL is not a valid PAIR peer: the local socket-type check faults
        // the establishment (and the server answers ERROR), exactly like the
        // ZMTP path - the check lives above the wire seam.
        await using var pull = new ZPullSocket();
        var failure = await Assert.ThrowsAsync<ZeroMqProtocolException>(
            () => pull.ConnectAsync($"ws://127.0.0.1:{port}/", token));
        Assert.Contains("is not accepted by local socket type 'PULL'", failure.Message);
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

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>Reads one whole WebSocket message into a fresh buffer.</summary>
    private static async Task<byte[]> ReceiveMessageAsync(WebSocket webSocket, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var buffer = new byte[4096];
        var received = await webSocket.ReceiveAsync(buffer, timeout.Token);
        Assert.True(received.EndOfMessage, "the ZWS peer never fragments");
        Assert.Equal(WebSocketMessageType.Binary, received.MessageType);
        return buffer[..received.Count];
    }

    private static Task SendZwsFrameAsync(WebSocket webSocket, byte[] message, CancellationToken token)
        => webSocket.SendAsync(message, WebSocketMessageType.Binary, true, token);

    /// <summary>Reads the HTTP header block; returns it plus any bytes that arrived after it.</summary>
    private static async Task<(string Header, byte[] Leftover)> ReadHttpHeaderAsync(NetworkStream stream,
        CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var buffer = new byte[4096];
        var read = 0;
        while (true)
        {
            var header = Encoding.ASCII.GetString(buffer, 0, read);
            var end = header.IndexOf("\r\n\r\n", StringComparison.Ordinal);
            if (end >= 0) return (header[..end], [.. buffer.AsSpan(end + 4, read - end - 4)]);

            var count = await stream.ReadAsync(buffer.AsMemory(read), timeout.Token);
            if (count == 0) throw new IOException("connection closed before the HTTP header completed");

            read += count;
        }
    }

    private static async Task<ZMessage?> ReadMessageAsync(
        ChannelReader<ZMessage> reader,
        CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            return await reader.ReadAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return null;
        }
    }
}
