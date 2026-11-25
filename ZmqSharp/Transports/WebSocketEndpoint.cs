namespace ZmqSharp.Transports;

/// <summary>
/// A ZWS endpoint (0035): <c>ws://host:port/path</c> or
/// <c>wss://host:port/path</c>. The host, port, and path are normalized
/// through <see cref="Uri"/>, matching the BCL WebSocket client: a default
/// port disappears from the canonical string, an IPv6 literal is bracketed, a
/// missing leading slash is added, a query is preserved, a fragment is
/// ignored, and escapes are canonicalized. Value equality matters: endpoint
/// teardown matches registrations by endpoint equality, mirroring
/// <see cref="System.Net.EndPoint"/>.
/// </summary>
public sealed class WebSocketEndpoint : IEquatable<WebSocketEndpoint>
{
    private readonly Uri _uri;

    public WebSocketEndpoint(string host, int port, string path, bool secure)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentOutOfRangeException.ThrowIfNegative(port);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(port, 65535);

        var authority = NormalizeHost(host);
        var requestPath = path.Length == 0 ? "/" : path;
        if (requestPath[0] != '/') requestPath = "/" + requestPath;

        Uri parsed;
        try
        {
            parsed = new Uri($"{(secure ? "wss" : "ws")}://{authority}:{port}{requestPath}");
        }
        catch (UriFormatException ex)
        {
            throw new ArgumentException(
                "the endpoint host, port, or path is not a valid ws URI", nameof(host), ex);
        }

        // The parsed authority must still be the requested host: a host that
        // Uri reinterpreted (user information, a slash, a query, a fragment)
        // silently addresses a different endpoint otherwise.
        if (!string.Equals(parsed.Host, authority, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("the endpoint host is not a valid ws host", nameof(host));

        // A fragment is not part of a WebSocket request (RFC 6455 section 3;
        // ClientWebSocket ignores it). Keep the canonical URI fragment-free so
        // Path, equality, and ToString stay consistent.
        _uri = parsed.Fragment.Length == 0
            ? parsed
            : new UriBuilder(parsed) { Fragment = string.Empty }.Uri;

        Host = _uri.Host;
        Port = _uri.Port;
        Path = _uri.PathAndQuery;
        Secure = secure;
    }

    public string Host { get; }

    public int Port { get; }

    public string Path { get; }

    /// <summary>Whether the endpoint addresses wss (TLS).</summary>
    public bool Secure { get; }

    /// <summary>The absolute URI the client WebSocket connects to.</summary>
    internal Uri Uri => _uri;

    public override string ToString() => _uri.ToString();

    /// <summary>Uri requires IPv6 literals in brackets; accept both forms.</summary>
    private static string NormalizeHost(string host)
        => host.Contains(':') && !host.StartsWith('[') ? $"[{host}]" : host;

    public bool Equals(WebSocketEndpoint? other)
        => other is not null &&
           string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) &&
           Port == other.Port &&
           string.Equals(Path, other.Path, StringComparison.Ordinal) &&
           Secure == other.Secure;

    public override bool Equals(object? obj) => obj is WebSocketEndpoint other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Host), Port, Path, Secure);

    public static bool operator ==(WebSocketEndpoint? left, WebSocketEndpoint? right)
        => left?.Equals(right) ?? right is null;

    public static bool operator !=(WebSocketEndpoint? left, WebSocketEndpoint? right) => !(left == right);
}
