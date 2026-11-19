namespace ZmqSharp.Transports;

/// <summary>
/// The bound listener surface a server transport exposes after BindAsync
/// (0036): accepts peers and reports each accepted connection via OnAccept
/// after StartAsync runs. The currency is the message connection (0034);
/// a byte transport raises its <see cref="IZConnection"/>, which is one.
/// </summary>
public interface IZServerTransport : IDisposable
{
    event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept;

    ValueTask StartAsync(CancellationToken token = default);
}

/// <summary>
/// The listen-only transport factory seam (0036): BindAsync yields a bound
/// transport whose accepted peers surface through OnAccept after StartAsync
/// runs. A listen-only transport implements this interface alone; a
/// connect-only transport implements <see cref="IZClientTransport{TSelf,TEndpoint}"/>
/// alone. A full-duplex transport implements both, and callers that need both
/// directions declare the two constraints together.
/// </summary>
public interface IZServerTransport<TSelf, in TEndpoint> : IZServerTransport
    where TSelf : IZServerTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint,
        ZTransportContext context,
        CancellationToken token = default);
}
