using System.Net;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// String-endpoint facade over the generic transport core: parses
/// "tcp://host:port" and dispatches to the matching generic transport.
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

    private static async Task<IPEndPoint> ParseEndpointAsync(string endpoint, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        var uri = new Uri(endpoint);
        if (!uri.Scheme.Equals("tcp", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException($"unsupported endpoint scheme '{uri.Scheme}' in '{endpoint}'");
        }

        int port = uri.Port;
        if (IPAddress.TryParse(uri.Host, out var address))
        {
            return new IPEndPoint(address, port);
        }

        var addresses = await Dns.GetHostAddressesAsync(uri.Host, token);
        if (addresses.Length == 0)
        {
            throw new InvalidOperationException($"could not resolve endpoint host '{uri.Host}'");
        }

        return new IPEndPoint(addresses[0], port);
    }
}
