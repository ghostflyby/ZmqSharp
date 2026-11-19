namespace ZmqSharp.Transports;

/// <summary>
/// The connect-only transport seam (0036): a transport that originates
/// connections but never accepts them. <see cref="IZServerTransport{TSelf,TEndpoint}"/>
/// is the symmetric listen-only seam; a full-duplex transport implements both,
/// and callers that need both directions declare the two constraints
/// together. A client-only transport implements this interface alone and
/// therefore advertises no accept, start, or bind members at all.
/// </summary>
public interface IZClientTransport<TSelf, in TEndpoint>
    where TSelf : IZClientTransport<TSelf, TEndpoint>
{
    /// <summary>
    /// Connects and yields a message connection. A byte transport composes
    /// its connection from <paramref name="context"/>; a native message
    /// transport already owns its units and ignores the framing.
    /// </summary>
    static abstract ValueTask<IZMessageConnection> ConnectAsync(
        TEndpoint endpoint,
        ZTransportContext context,
        CancellationToken token = default);
}
