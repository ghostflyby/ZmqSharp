using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// Common contract of every socket surface: endpoint management only (0024).
/// Send is not on the interface - each socket type exposes its own public
/// send surface (PUSH/PAIR/PUB/DEALER expose SendAsync, REQ exposes
/// RequestAsync, REP exposes SendReplyAsync, ROUTER exposes the identity
/// overload, receive-only types expose nothing).
/// </summary>
public interface IZSocket : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    void Unbind<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    void Disconnect<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    ValueTask UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    ValueTask DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task BindAsync(string endpoint, CancellationToken token = default);
    Task ConnectAsync(string endpoint, CancellationToken token = default);
    ValueTask UnbindAsync(string endpoint, CancellationToken token = default);
    ValueTask DisconnectAsync(string endpoint, CancellationToken token = default);
}
