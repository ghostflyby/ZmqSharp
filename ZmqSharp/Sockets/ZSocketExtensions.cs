using System.Net;
using System.Net.Sockets;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// String-endpoint facade over the generic transport core: parses
/// "tcp://host:port" and "ipc://path" and dispatches to the matching generic
/// transport (0015 section 5.2).
/// </summary>
public static class ZSocketExtensions
{
    extension(IZSocket socket)
    {
        public Task ConnectAsync(string endpoint, CancellationToken token = default)
        {
            return ConnectAsyncCore(socket, endpoint, token);
        }

        public Task BindAsync(string endpoint, CancellationToken token = default)
        {
            return BindAsyncCore(socket, endpoint, token);
        }
    }

    private static async Task ConnectAsyncCore(IZSocket socket, string endpoint, CancellationToken token)
    {
        var parsed = await ParseEndpointAsync(endpoint, token);
        await socket.ConnectAsync<EndPoint, SocketTransport>(parsed, token);
    }

    private static async Task BindAsyncCore(IZSocket socket, string endpoint, CancellationToken token)
    {
        var parsed = await ParseEndpointAsync(endpoint, token);
        await socket.BindAsync<EndPoint, SocketTransport>(parsed, token);
    }

    private static async Task<EndPoint> ParseEndpointAsync(string endpoint, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        var uri = new Uri(endpoint);
        if (uri.Scheme.Equals("tcp", StringComparison.OrdinalIgnoreCase))
            return await ParseTcpAsync(uri, token);

        if (uri.Scheme.Equals("ipc", StringComparison.OrdinalIgnoreCase))
            return ParseIpc(uri);

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
    /// directory (0015 section 5.2).
    /// </summary>
    private static UnixDomainSocketEndPoint ParseIpc(Uri uri)
    {
        // A relative form such as "ipc://my.sock" puts the path in the host
        // slot (PathAndQuery is empty); an absolute form such as
        // "ipc:///tmp/foo.sock" keeps it in the path.
        var path = uri.PathAndQuery;
        if (path.Length == 0) path = uri.Authority;

        if (!path.StartsWith('/'))
            path = Path.Combine(Path.GetTempPath(), path);

        return new UnixDomainSocketEndPoint(path);
    }
}
