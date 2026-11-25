using System.Net.WebSockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The ZWS client transport (0035): connects a WebSocket endpoint with the
/// BCL <see cref="ClientWebSocket"/> and offers the proposed RFC 45
/// <c>ZWS2.0+NULL</c> subprotocol plus the RFC-mandated no-mechanism
/// <c>ZWS2.0</c> fallback. The proposal changes the version/mechanism
/// separator from '/' to '+', which makes the mechanism name a valid RFC 6455
/// token - the reason the BCL client can perform the upgrade at all (a
/// '/'-separated name is rejected before the request is written).
/// </summary>
/// <remarks>
/// Client-only by design: the listening half of ZWS would require a
/// hand-rolled WebSocket server, which this library deliberately does not
/// ship. Implementers get the compile-time connection seam
/// <see cref="IZClientTransport{TSelf,TEndpoint}"/>; the string endpoint
/// facade reports <see cref="NotSupportedException"/> on bind.
/// </remarks>
public sealed class WebSocketTransport : IZClientTransport<WebSocketTransport, WebSocketEndpoint>
{
    internal WebSocketTransport()
    {
    }

    /// <summary>Client connect with the default context (shared pool).</summary>
    public static ValueTask<IZMessageConnection> ConnectAsync(
        WebSocketEndpoint endpoint, CancellationToken token = default)
        => ConnectAsync(endpoint, ZTransportContext.Default, token);

    /// <summary>
    /// Connects the endpoint and performs the ZWS/WebSocket upgrade. wss is
    /// handled by <see cref="ClientWebSocket"/>'s TLS stack, including the
    /// system certificate policy.
    /// </summary>
    public static async ValueTask<IZMessageConnection> ConnectAsync(
        WebSocketEndpoint endpoint,
        ZTransportContext context,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        var webSocket = new ClientWebSocket();
        try
        {
            webSocket.Options.AddSubProtocol(ZwsProtocol.NullSubProtocol);
            // RFC 45 requires every ZWS2.0 implementation to support the
            // no-mechanism profile; current libzmq servers select it as the
            // fallback when they do not know the proposed '+' spelling. Offer
            // the mechanism first so a revision-aware server prefers it.
            webSocket.Options.AddSubProtocol(ZwsProtocol.NoMechanismSubProtocol);
            // ZWS v1 issues no WebSocket keepalive probes; the protocol's own
            // traffic is the liveness signal.
            webSocket.Options.KeepAliveInterval = TimeSpan.Zero;
            await webSocket.ConnectAsync(endpoint.Uri, token);

            if (webSocket.SubProtocol is not { } selected ||
                (!string.Equals(selected, ZwsProtocol.NullSubProtocol, StringComparison.Ordinal) &&
                 !string.Equals(selected, ZwsProtocol.NoMechanismSubProtocol, StringComparison.Ordinal)))
                throw new WebSocketException(
                    $"the server negotiated WebSocket subprotocol '{webSocket.SubProtocol ?? "<none>"}'; ZWS requires '{ZwsProtocol.NullSubProtocol}' or '{ZwsProtocol.NoMechanismSubProtocol}'");

            return new ZwsConnection(webSocket, selected, context.Pool);
        }
        catch
        {
            webSocket.Dispose();
            throw;
        }
    }
}
