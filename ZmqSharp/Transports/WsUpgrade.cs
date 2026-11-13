using System.Buffers;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;

namespace ZmqSharp.Transports;

/// <summary>
/// The server half of the ZWS HTTP/1.1 upgrade (0033): parses the WebSocket
/// handshake request, selects the ZWS subprotocol, computes the
/// Sec-WebSocket-Accept key, and answers 101 or a terminal 400. Strictly
/// bounded parsing on a cold path, so the header block is decoded to a
/// string once and parsed with string operations; the wire bytes are locked
/// by vectors from RFC 6455 and libzmq's ws_engine.
/// </summary>
internal static class WsUpgrade
{
    /// <summary>
    /// The only subprotocol this implementation negotiates: ZMTP 3.0 NULL
    /// security over WebSocket frames (RFC 45 + libzmq's ws_engine).
    /// </summary>
    internal const string NullSubprotocol = "ZWS2.0/NULL";

    private const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private const int MaxRequestBytes = 16 * 1024;

    internal readonly record struct Request(string Path, string SecWebSocketKey, string[] Protocols);

    /// <summary>
    /// Reads one HTTP request from the stream (bounded by
    /// <see cref="MaxRequestBytes"/>) and performs the WebSocket upgrade when
    /// the request is a valid ZWS handshake offering
    /// <see cref="NullSubprotocol"/>. Returns null after writing a terminal
    /// 400 (or on EOF with no request); the caller disposes the stream. The
    /// negotiated subprotocol is NOT handed to the BCL (its token grammar
    /// rejects '/', which libzmq's "ZWS2.0/NULL" contains); the caller
    /// carries it in the connection instead.
    /// </summary>
    internal static async ValueTask<WebSocket?> AcceptAsync(Stream stream, CancellationToken token = default)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(MaxRequestBytes);
        try
        {
            var read = 0;
            while (true)
            {
                var headerEnd = buffer.AsSpan(0, read).IndexOf("\r\n\r\n"u8);
                if (headerEnd >= 0)
                {
                    var header = Encoding.ASCII.GetString(buffer.AsSpan(0, headerEnd));
                    if (!TryParseRequest(header, out var request) ||
                        !request.Protocols.Contains(NullSubprotocol, StringComparer.Ordinal))
                    {
                        await stream.WriteAsync(BuildBadRequest(), token);
                        return null;
                    }

                    var acceptKey = ComputeAcceptKey(request.SecWebSocketKey);
                    await stream.WriteAsync(BuildSwitchingProtocols(acceptKey), token);
                    // The header read may have consumed the peer's first
                    // pipelined WebSocket frame; replay it before the stream.
                    var leftover = buffer[(headerEnd + 4)..read];
                    var upgraded = new PrefixedStream([.. leftover], stream);
                    // Heartbeat pings (WS ping/pong) are out of scope for v1
                    // (0033): no keepalive probe is issued.
                    return WebSocket.CreateFromStream(upgraded, isServer: true, null, TimeSpan.Zero);
                }

                if (read >= MaxRequestBytes)
                {
                    await stream.WriteAsync(BuildBadRequest(), token);
                    return null;
                }

                var count = await stream.ReadAsync(buffer.AsMemory(read), token);
                if (count == 0) return null;

                read += count;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// The client half of the upgrade: writes a libzmq-shaped handshake
    /// request, reads and validates the 101 (status line, the echoed
    /// subprotocol, and the RFC 6455 accept key), and returns the upgraded
    /// WebSocket. ClientWebSocket cannot be used here: the BCL validates the
    /// subprotocol token grammar and rejects the '/' in "ZWS2.0/NULL"
    /// (libzmq's hand-rolled HTTP does not).
    /// </summary>
    internal static async ValueTask<WebSocket> ConnectAsync(
        Stream stream, string host, string path, CancellationToken token = default)
    {
        var key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
        var request =
            $"GET {path} HTTP/1.1\r\n" +
            $"Host: {host}\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Key: {key}\r\n" +
            $"Sec-WebSocket-Protocol: {NullSubprotocol}\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request), token);

        var (header, leftover) = await ReadResponseHeaderAsync(stream, token);
        if (!header.StartsWith("HTTP/1.1 101", StringComparison.Ordinal) ||
            !TryReadHeader(header, "Sec-WebSocket-Protocol", out var negotiated) ||
            !string.Equals(negotiated.Trim(), NullSubprotocol, StringComparison.Ordinal) ||
            !TryReadHeader(header, "Sec-WebSocket-Accept", out var accept) ||
            !string.Equals(accept.Trim(), ComputeAcceptKey(key), StringComparison.Ordinal))
            throw new IOException("the server did not complete the ZWS WebSocket upgrade");

        // The header read may have consumed the server's first pipelined
        // frame (its READY); replay it before the stream. The client side
        // applies RFC 6455 masking, like libzmq's ws_encoder.
        var upgraded = new PrefixedStream(leftover, stream);
        return WebSocket.CreateFromStream(upgraded, isServer: false, null, TimeSpan.Zero);
    }

    private static async Task<(string Header, byte[] Leftover)> ReadResponseHeaderAsync(
        Stream stream, CancellationToken token)
    {
        var buffer = new byte[4096];
        var read = 0;
        while (true)
        {
            var header = Encoding.ASCII.GetString(buffer, 0, read);
            var end = header.IndexOf("\r\n\r\n", StringComparison.Ordinal);
            if (end >= 0) return (header[..end], [.. buffer.AsSpan(end + 4, read - end - 4)]);

            if (read == buffer.Length) throw new IOException("the upgrade response header is too large");

            var count = await stream.ReadAsync(buffer.AsMemory(read), token);
            if (count == 0) throw new IOException("connection closed during the WebSocket upgrade");

            read += count;
        }
    }

    private static bool TryReadHeader(string header, string name, out string value)
    {
        foreach (var line in header.Split("\r\n"))
        {
            var colon = line.IndexOf(':');
            if (colon <= 0 || !string.Equals(line[..colon], name, StringComparison.OrdinalIgnoreCase)) continue;

            value = line[(colon + 1)..].TrimStart();
            return true;
        }

        value = string.Empty;
        return false;
    }

    /// <summary>
    /// Validates a GET HTTP/1.1 upgrade request: Upgrade: websocket,
    /// Connection containing the upgrade token, Sec-WebSocket-Key present,
    /// Sec-WebSocket-Version 13, and a Sec-WebSocket-Protocol offer list.
    /// </summary>
    internal static bool TryParseRequest(string header, out Request request)
    {
        request = default;
        var lineEnd = header.IndexOf("\r\n", StringComparison.Ordinal);
        if (lineEnd < 0) return false;

        var requestLine = header[..lineEnd];
        if (!requestLine.StartsWith("GET ", StringComparison.Ordinal)) return false;

        var versionStart = requestLine.LastIndexOf(' ');
        if (versionStart < 0) return false;

        if (!string.Equals(requestLine[(versionStart + 1)..], "HTTP/1.1", StringComparison.Ordinal)) return false;

        var path = requestLine[4..versionStart];
        if (path.Length == 0 || path[0] != '/' || path.Contains(' ')) return false;

        string? key = null;
        string[]? protocols = null;
        var hasUpgrade = false;
        var hasConnectionUpgrade = false;
        var hasVersion13 = false;
        var rest = header[(lineEnd + 2)..];
        while (rest.Length > 0)
        {
            var next = rest.IndexOf("\r\n", StringComparison.Ordinal);
            var line = next < 0 ? rest : rest[..next];
            rest = next < 0 ? string.Empty : rest[(next + 2)..];
            if (line.Length == 0) continue;

            var colon = line.IndexOf(':');
            if (colon <= 0) return false;

            var name = line[..colon];
            var value = line[(colon + 1)..].TrimStart();

            if (string.Equals(name, "Upgrade", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.Equals(value, "websocket", StringComparison.Ordinal)) return false;

                hasUpgrade = true;
            }
            else if (string.Equals(name, "Connection", StringComparison.OrdinalIgnoreCase))
            {
                // RFC 6455 section 4.1: the Connection token match is
                // case-insensitive; libzmq sends "Connection: Upgrade".
                foreach (var token in value.Split(','))
                    if (token.Trim().Equals("upgrade", StringComparison.OrdinalIgnoreCase))
                        hasConnectionUpgrade = true;
            }
            else if (string.Equals(name, "Sec-WebSocket-Key", StringComparison.OrdinalIgnoreCase))
            {
                if (value.Length == 0) return false;

                key = value;
            }
            else if (string.Equals(name, "Sec-WebSocket-Version", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.Equals(value, "13", StringComparison.Ordinal)) return false;

                hasVersion13 = true;
            }
            else if (string.Equals(name, "Sec-WebSocket-Protocol", StringComparison.OrdinalIgnoreCase))
            {
                protocols = [.. value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)];
            }
        }

        if (!hasUpgrade || !hasConnectionUpgrade || !hasVersion13 || key is null || protocols is null) return false;

        request = new Request(path, key, protocols);
        return true;
    }

    /// <summary>RFC 6455 Sec-WebSocket-Accept: Base64(SHA1(key + GUID)).</summary>
    internal static string ComputeAcceptKey(string secWebSocketKey)
    {
        var hash = SHA1.HashData(Encoding.ASCII.GetBytes(secWebSocketKey + WebSocketGuid));
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// The 101 response, echoing the selected subprotocol exactly as
    /// libzmq's ws_engine does (header order included).
    /// </summary>
    internal static byte[] BuildSwitchingProtocols(string acceptKey)
        => Encoding.ASCII.GetBytes(
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Accept: {acceptKey}\r\n" +
            $"Sec-WebSocket-Protocol: {NullSubprotocol}\r\n" +
            "\r\n");

    internal static byte[] BuildBadRequest()
        => Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
}
