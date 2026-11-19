using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// A minimal ZWS server for client tests only: it performs the HTTP/1.1
/// upgrade and then hands frame handling to the BCL
/// <c>WebSocket.CreateFromStream</c> server mode. The library itself
/// ships no WebSocket server (0035); this harness exists solely to give the
/// client a real peer.
/// </summary>
internal sealed class ZwsTestServer : IAsyncDisposable
{
    internal const string SubProtocol = "ZWS2.0+NULL";

    private const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private const int MaxRequestBytes = 16 * 1024;

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _cts = new();

    private ZwsTestServer()
    {
        _listener.Start();
    }

    public static ZwsTestServer Start() => new();

    private int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public string Endpoint(string path = "/") => $"ws://127.0.0.1:{Port}{path}";

    /// <summary>
    /// Accepts one client and completes the upgrade. The request header is
    /// returned so tests can assert the offered subprotocol; the leftover
    /// bytes (if the client pipelined a frame) are replayed before the
    /// upgraded stream.
    /// </summary>
    public async Task<ZwsTestSession> AcceptAsync(
        string? selectedProtocol = SubProtocol, CancellationToken token = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _cts.Token);
        var client = await _listener.AcceptTcpClientAsync(linked.Token);
        try
        {
            var stream = client.GetStream();
            var (request, leftover) = await ReadRequestAsync(stream, linked.Token);
            var key = ReadHeader(request, "Sec-WebSocket-Key")
                      ?? throw new InvalidOperationException("the client did not send Sec-WebSocket-Key");

            var response = new StringBuilder()
                .Append("HTTP/1.1 101 Switching Protocols\r\n")
                .Append("Upgrade: websocket\r\n")
                .Append("Connection: Upgrade\r\n")
                .Append("Sec-WebSocket-Accept: ").Append(ComputeAcceptKey(key)).Append("\r\n");
            if (selectedProtocol is not null)
                response.Append("Sec-WebSocket-Protocol: ").Append(selectedProtocol).Append("\r\n");
            response.Append("\r\n");

            await stream.WriteAsync(Encoding.ASCII.GetBytes(response.ToString()), linked.Token);

            Stream upgraded = leftover.Length == 0 ? stream : new PrefixStream(leftover, stream);
            var socket = WebSocket.CreateFromStream(
                upgraded, isServer: true, selectedProtocol, TimeSpan.Zero);
            return new ZwsTestSession(socket, request, client);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        _cts.Dispose();
    }

    private static async Task<(string Request, byte[] Leftover)> ReadRequestAsync(
        Stream stream, CancellationToken token)
    {
        var buffer = new byte[MaxRequestBytes];
        var read = 0;
        while (true)
        {
            var end = buffer.AsSpan(0, read).IndexOf("\r\n\r\n"u8);
            if (end >= 0)
            {
                var request = Encoding.ASCII.GetString(buffer, 0, end);
                return (request, buffer.AsSpan(end + 4, read - end - 4).ToArray());
            }

            if (read == buffer.Length) throw new InvalidOperationException("the upgrade request is too large");

            var count = await stream.ReadAsync(buffer.AsMemory(read), token);
            if (count == 0) throw new IOException("the client closed before the upgrade request completed");

            read += count;
        }
    }

    internal static string? ReadHeader(string request, string name)
    {
        foreach (var line in request.Split("\r\n"))
        {
            var colon = line.IndexOf(':');
            if (colon <= 0 || !string.Equals(line[..colon], name, StringComparison.OrdinalIgnoreCase)) continue;

            return line[(colon + 1)..].Trim();
        }

        return null;
    }

    private static string ComputeAcceptKey(string secWebSocketKey)
    {
        var hash = SHA1.HashData(Encoding.ASCII.GetBytes(secWebSocketKey + WebSocketGuid));
        return Convert.ToBase64String(hash);
    }

    /// <summary>A non-seekable read prefix followed by the inner stream.</summary>
    private sealed class PrefixStream(byte[] prefix, Stream inner) : Stream
    {
        private int _position;

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_position >= prefix.Length) return inner.ReadAsync(buffer, cancellationToken);

            var count = Math.Min(buffer.Length, prefix.Length - _position);
            prefix.AsMemory(_position, count).CopyTo(buffer);
            _position += count;
            return ValueTask.FromResult(count);
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => inner.WriteAsync(buffer, cancellationToken);

        public override Task FlushAsync(CancellationToken cancellationToken)
            => inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override void Flush() => inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}

/// <summary>One accepted test peer: the upgraded socket plus its raw request.</summary>
internal sealed class ZwsTestSession(WebSocket socket, string request, TcpClient client) : IAsyncDisposable
{
    public WebSocket Socket { get; } = socket;

    public string Request { get; } = request;

    public async ValueTask DisposeAsync()
    {
        try
        {
            Socket.Dispose();
        }
        finally
        {
            client.Dispose();
        }
    }
}

/// <summary>ZWS frame helpers for the test server side (flag byte + body).</summary>
internal static class ZwsTestWire
{
    public static async Task SendFrameAsync(
        WebSocket socket, byte flag, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        var message = new byte[1 + body.Length];
        message[0] = flag;
        body.CopyTo(message.AsMemory(1));
        await socket.SendAsync(message, WebSocketMessageType.Binary, true, token);
    }

    public static Task SendCommandAsync(WebSocket socket, ReadOnlyMemory<byte> body, CancellationToken token)
        => SendFrameAsync(socket, 0x02, body, token);

    public static async Task<(byte Flag, byte[] Body)> ReceiveFrameAsync(
        WebSocket socket, CancellationToken token)
    {
        var message = await ReceiveMessageAsync(socket, token);
        if (message.Length == 0) return (0, []);
        return (message[0], message[1..]);
    }

    public static async Task<byte[]> ReceiveMessageAsync(WebSocket socket, CancellationToken token)
    {
        var buffer = new byte[4096];
        var filled = 0;
        while (true)
        {
            if (filled == buffer.Length) Array.Resize(ref buffer, buffer.Length * 2);

            var result = await socket.ReceiveAsync(buffer.AsMemory(filled), token);
            if (result.MessageType == WebSocketMessageType.Close) return [];

            filled += result.Count;
            if (result.EndOfMessage) return buffer[..filled];
        }
    }
}
