using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;

namespace ZmqSharp.Transports;

/// <summary>
/// ZWS transport (0033): ConnectAsync upgrades a client WebSocket offering
/// the ZWS2.0/NULL subprotocol (wss goes through TLS automatically); BindAsync
/// listens on TCP and upgrades accepted sockets with a hand-rolled HTTP/1.1
/// handshake before surfacing <see cref="WebSocketConnection"/> peers via
/// OnAccept. The ZMTP greeting never flows here - the subprotocol carries the
/// security mechanism, so the connections speak the ZWS wire protocol and are
/// driven by the runtime's protocol-connection path.
/// </summary>
public sealed class WebSocketTransport : IZTransport<WebSocketTransport, WebSocketEndpoint>
{
    private readonly Socket _listener;
    private Func<IZConnection, CancellationToken, ValueTask>? _onAccept;
    private int _closed;

    private WebSocketTransport(Socket listener)
    {
        _listener = listener;
    }

    public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
    {
        add => _onAccept += value;
        remove => _onAccept -= value;
    }

    public static async ValueTask<IZConnection> ConnectAsync(
        WebSocketEndpoint endpoint,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        var host = endpoint.Host.StartsWith('[') ? endpoint.Host[1..^1] : endpoint.Host;
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(host, endpoint.Port, token);
            Stream stream = new NetworkStream(socket, ownsSocket: true);
            if (endpoint.Secure)
            {
                var secure = new SslStream(stream, leaveInnerStreamOpen: false);
                try
                {
                    await secure.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                    {
                        TargetHost = host,
                    }, token);
                    stream = secure;
                }
                catch
                {
                    await secure.DisposeAsync();
                    throw;
                }
            }

            // ClientWebSocket cannot carry the ZWS subprotocol (the BCL token
            // grammar rejects its '/'), so the upgrade is hand-rolled exactly
            // like the server half (0033).
            var webSocket = await WsUpgrade.ConnectAsync(stream, endpoint.Host, endpoint.Path, token);
            return new WebSocketConnection(webSocket, WsUpgrade.NullSubprotocol);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public static async ValueTask<WebSocketTransport> BindAsync(
        WebSocketEndpoint endpoint,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        var address = await ResolveAsync(endpoint, token);
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            // A wildcard bind accepts both IPv6 and IPv4 peers.
            if (endpoint.Host.Equals("*", StringComparison.Ordinal) && socket.AddressFamily == AddressFamily.InterNetworkV6)
                socket.DualMode = true;

            socket.NoDelay = true;
            socket.Bind(new IPEndPoint(address, endpoint.Port));
            socket.Listen();
            return new WebSocketTransport(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public async ValueTask StartAsync(CancellationToken token = default)
    {
        while (Volatile.Read(ref _closed) == 0)
        {
            Socket accepted;
            try
            {
                accepted = await _listener.AcceptAsync(token);
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                accepted.NoDelay = true;
                // The stream owns the socket: disposing it (directly or
                // through the WebSocket) closes the accepted connection.
                var stream = new NetworkStream(accepted, ownsSocket: true);
                var webSocket = await WsUpgrade.AcceptAsync(stream, token);
                if (webSocket is not { } connection)
                {
                    // Rejected (a terminal 400 was written) or EOF: close.
                    stream.Dispose();
                    continue;
                }

                if (_onAccept is { } handler)
                    await handler(new WebSocketConnection(connection, WsUpgrade.NullSubprotocol), token);
                else
                    connection.Dispose();
            }
            catch (Exception ex) when (!token.IsCancellationRequested &&
                                       ex is SocketException or ObjectDisposedException or IOException
                                           or WebSocketException or OperationCanceledException)
            {
                // The peer reset or failed its upgrade before the connection
                // could be set up; drop it without faulting the accept loop.
                accepted.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;

        _listener.Dispose();
    }

    /// <summary>
    /// Resolves the bind address: "*" is the wildcard, literal IPs (v6
    /// brackets stripped by the parser) parse directly, and names resolve to
    /// the first address, mirroring the TCP transport's policy.
    /// </summary>
    private static async ValueTask<IPAddress> ResolveAsync(WebSocketEndpoint endpoint, CancellationToken token)
    {
        if (endpoint.Host.Equals("*", StringComparison.Ordinal)) return IPAddress.IPv6Any;

        var host = endpoint.Host.StartsWith('[') ? endpoint.Host[1..^1] : endpoint.Host;
        if (IPAddress.TryParse(host, out var address)) return address;

        var addresses = await Dns.GetHostAddressesAsync(host, token);
        return addresses.Length == 0
            ? throw new InvalidOperationException($"could not resolve endpoint host '{host}'")
            : addresses[0];
    }
}
