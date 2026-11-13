namespace ZmqSharp.Transports;

/// <summary>
/// A ZWS endpoint (0033): <c>ws://host:port/path</c> or <c>wss://host:port/path</c>.
/// The path (including any query) is preserved verbatim because the
/// WebSocket request line carries it - port-sharing deployments route on it.
/// Value equality matters: endpoint teardown matches registrations by
/// endpoint equality, mirroring <see cref="System.Net.EndPoint"/>.
/// </summary>
public sealed class WebSocketEndpoint(string host, int port, string path, bool secure)
    : IEquatable<WebSocketEndpoint>
{
    public string Host { get; } = string.IsNullOrEmpty(host)
        ? throw new ArgumentException("endpoint host is required", nameof(host))
        : host;

    public int Port { get; } = port is >= 0 and <= 65535
        ? port
        : throw new ArgumentOutOfRangeException(nameof(port), "port is out of range");

    public string Path { get; } = path.Length == 0 ? "/" : path;

    /// <summary>Whether the endpoint addresses wss (TLS).</summary>
    public bool Secure { get; } = secure;

    public override string ToString()
        => $"{(Secure ? "wss" : "ws")}://{Host}:{Port}{Path}";

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
        => left is null ? right is null : left.Equals(right);

    public static bool operator !=(WebSocketEndpoint? left, WebSocketEndpoint? right) => !(left == right);
}
