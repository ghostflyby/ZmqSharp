using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// Common contract of every socket surface: endpoint management and direct
/// send. Receive differs by surface (callback vs channel).
/// </summary>
public interface IZSocketBase : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task CloseAsync(CancellationToken token = default);

    /// <summary>Direct send: transfers ownership; the socket disposes the message after routing.</summary>
    ValueTask SendAsync(IZMessage message, CancellationToken token = default);

    /// <summary>Direct send: copies the payload into an owned message before routing.</summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
}
