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

    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;
}
