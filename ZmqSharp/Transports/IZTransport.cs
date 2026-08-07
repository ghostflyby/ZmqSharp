using ZmqSharp.Sockets;

namespace ZmqSharp.Transports;

/// <summary>
/// Byte transport with unified send/receive over a Stream. After BindAsync the
/// same transport acts as a listener: AcceptAsync yields connected transports.
/// </summary>
public interface IZTransport : IDisposable
{
    /// <summary>Non-null when connected (ConnectAsync or AcceptAsync).</summary>
    Stream? Stream { get; }

    /// <summary>Accepts a connected transport; valid only after BindAsync.</summary>
    ValueTask<IZTransport> AcceptAsync(CancellationToken token = default);
}

/// <summary>
/// Generic transport factory (core contract): transports plug in with typed
/// endpoints and compile-time selection; both ConnectAsync and BindAsync return
/// the same transport type.
/// </summary>
public interface IZTransport<TSelf, in TEndpoint> : IZTransport
    where TSelf : IZTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<TSelf> ConnectAsync(
        IZSocket zsocket,
        TEndpoint endpoint,
        CancellationToken token = default);

    static abstract ValueTask<TSelf> BindAsync(
        IZSocket zsocket,
        TEndpoint endpoint,
        CancellationToken token = default);
}
