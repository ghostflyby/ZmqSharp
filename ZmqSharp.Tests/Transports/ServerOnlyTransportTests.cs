using System.Net;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// The listen-only transport seam (0036): a transport that implements only
/// <see cref="IZServerTransport{TSelf,TEndpoint}"/> can bind and unbind
/// without carrying a connect factory or a throwing BindAsync placeholder.
/// </summary>
public sealed class ServerOnlyTransportTests
{
    [Fact(Timeout = 15_000)]
    public async Task ServerOnlyTransport_CanBindAndUnbind()
    {
        await using var socket = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);

        await socket.BindAsync<EndPoint, ServerOnlyTransport>(endpoint, token);
        await socket.UnbindAsync<EndPoint, ServerOnlyTransport>(endpoint, token);
    }

    private sealed class ServerOnlyTransport : IZServerTransport<ServerOnlyTransport, EndPoint>
    {
        public event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept
        {
            add { }
            remove { }
        }

        public static ValueTask<ServerOnlyTransport> BindAsync(
            EndPoint endpoint, ZTransportContext context, CancellationToken token = default)
            => ValueTask.FromResult(new ServerOnlyTransport());

        public async ValueTask StartAsync(CancellationToken token = default)
            => await Task.Delay(Timeout.InfiniteTimeSpan, token);

        public void Dispose() { }
    }
}
