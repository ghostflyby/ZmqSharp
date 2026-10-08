using System.Net;
using System.Net.Sockets;

namespace ZmqSharp.Sockets;

/// <summary>
/// Parses the string endpoints used by the socket's transport facade
/// (0015 section 5.2, 0029).
/// </summary>
internal static class ZEndpointParser
{
    internal static async Task<EndPoint> ParseEndpointAsync(string endpoint, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        var uri = new Uri(endpoint);
        if (uri.Scheme.Equals("tcp", StringComparison.OrdinalIgnoreCase))
            return await ParseTcpAsync(uri, token);

        if (uri.Scheme.Equals("ipc", StringComparison.OrdinalIgnoreCase))
            return ParseIpc(endpoint, uri);

        throw new NotSupportedException($"unsupported endpoint scheme '{uri.Scheme}' in '{endpoint}'");
    }

    private static async Task<IPEndPoint> ParseTcpAsync(Uri uri, CancellationToken token)
    {
        var port = uri.Port;
        if (IPAddress.TryParse(uri.Host, out var address)) return new IPEndPoint(address, port);

        var addresses = await Dns.GetHostAddressesAsync(uri.Host, token);
        if (addresses.Length == 0) throw new InvalidOperationException($"could not resolve endpoint host '{uri.Host}'");

        return new IPEndPoint(addresses[0], port);
    }

    /// <summary>
    /// "ipc://path" addresses a Unix domain socket: an absolute path keeps its
    /// leading slash ("ipc:///tmp/foo" -> "/tmp/foo"); a relative path resolves
    /// against the system temp directory, mirroring libzmq's default IPC
    /// directory (0015 section 5.2). "ipc://@name" addresses the Linux
    /// abstract namespace (libzmq's convention, 0020): no filesystem entry,
    /// cleaned up when the socket closes.
    /// </summary>
    private static UnixDomainSocketEndPoint ParseIpc(string endpoint, Uri uri)
    {
        // The URI parser treats '@' as userinfo and drops it, so the abstract
        // form must be detected on the raw string. libzmq stores an abstract
        // address with a leading null byte; the BCL's UnixDomainSocketEndPoint
        // maps that literal '\0' to the abstract namespace (it does not
        // convert '@').
        const string prefix = "ipc://";
        if (endpoint.AsSpan(prefix.Length).StartsWith('@'))
        {
            var name = endpoint.AsSpan(prefix.Length + 1);
            if (name.IsEmpty)
                throw new ArgumentException("ipc abstract namespace requires a name", nameof(endpoint));

            return new UnixDomainSocketEndPoint("\0" + name.ToString());
        }

        // The URI parser splits the two forms differently: an absolute form
        // such as "ipc:///tmp/foo.sock" lands in AbsolutePath ("/tmp/foo.sock",
        // query excluded), while a relative form such as "ipc://my.sock" puts
        // the path in Host and leaves AbsolutePath at "/".
        var path = uri.AbsolutePath;
        if ((path.Length == 0 || path == "/") && uri.Host.Length > 0) path = uri.Host;

        if (!path.StartsWith('/'))
            path = Path.Combine(Path.GetTempPath(), path);

        return new UnixDomainSocketEndPoint(path);
    }
}
