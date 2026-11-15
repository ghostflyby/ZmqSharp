namespace ZmqSharp.Transports;

/// <summary>
/// A bound transport: accepts peers and reports each accepted connection via
/// OnAccept after StartAsync runs. The currency is the message connection
/// (0034); a byte transport raises its <see cref="IZConnection"/>, which is
/// one.
/// </summary>
public interface IZTransport : IDisposable
{
    event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept;

    ValueTask StartAsync(CancellationToken token = default);
}

/// <summary>
/// The one transport factory (0034): ConnectAsync yields a connected message
/// connection; BindAsync yields a listening transport (OnAccept + StartAsync).
/// A byte transport composes its connection from <see cref="ZTransportContext"/>
/// - the framing is captured at construction, so the connection owns exactly
/// one dialect. Transport strategy and socket queues are not its concern.
/// </summary>
public interface IZTransport<TSelf, in TEndpoint> : IZTransport
    where TSelf : IZTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<IZMessageConnection> ConnectAsync(
        TEndpoint endpoint,
        ZTransportContext context,
        CancellationToken token = default);

    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint,
        ZTransportContext context,
        CancellationToken token = default);
}
