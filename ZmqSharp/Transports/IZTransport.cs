namespace ZmqSharp.Transports;

/// <summary>
/// A bound transport: accepts peers and reports each accepted connection via
/// OnAccept after StartAsync runs.
/// </summary>
public interface IZTransport : IDisposable
{
    event Func<IZConnection, CancellationToken, ValueTask>? OnAccept;

    ValueTask StartAsync(CancellationToken token = default);
}

/// <summary>
/// Generic transport factory: ConnectAsync yields a connected connection;
/// BindAsync yields a listening transport (OnAccept + StartAsync). Transport
/// strategy and socket queues are not its concern.
/// </summary>
public interface IZTransport<TSelf, in TEndpoint> : IZTransport
    where TSelf : IZTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<IZConnection> ConnectAsync(
        TEndpoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default);

    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default);
}
