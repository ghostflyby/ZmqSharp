namespace ZmqSharp.Transports;

/// <summary>
/// A frame-layer connection (0033): reads yield whole frames (ownership
/// transferred to the caller, who disposes them); null means the peer closed
/// - distinct from a frame with an empty body, which is a valid frame. Writes
/// deliver whole frames; completion means the frame was handed off, and the
/// sender retains disposal (the runtime disposes messages after sending,
/// exactly as on the byte path). Concurrency, abort and dispose follow the
/// byte contract (0030 section 3): one reader and one writer may overlap;
/// <see cref="Abort"/> is idempotent and unblocks pending I/O immediately;
/// dispose releases resources once operations have finished.
/// </summary>
public interface IZFrameConnection : IDisposable
{
    /// <summary>Reads one whole frame. Null on peer close (EOF).</summary>
    ValueTask<ZWireFrame?> ReadFrameAsync(CancellationToken token = default);

    /// <summary>Writes one whole frame; completion means delivered.</summary>
    ValueTask WriteFrameAsync(ZWireFrame frame, CancellationToken token = default);

    /// <summary>Idempotently aborts in-flight I/O without a close handshake.</summary>
    void Abort();
}

/// <summary>
/// A bound frame-layer transport: accepts peers and reports each accepted
/// frame connection via OnAccept after StartAsync runs - the frame-currency
/// sibling of <see cref="IZTransport"/> (the two listener contracts cannot
/// merge: their event currencies differ, mirroring separate listener shapes
/// per currency in the BCL's connection stack).
/// </summary>
public interface IZFrameTransport : IDisposable
{
    event Func<IZFrameConnection, CancellationToken, ValueTask>? OnAccept;

    ValueTask StartAsync(CancellationToken token = default);
}

/// <summary>
/// Generic frame-transport factory, mirroring
/// <see cref="IZTransport{TSelf,TEndpoint}"/>: ConnectAsync yields a
/// connected frame connection; BindAsync yields a listening frame transport.
/// </summary>
public interface IZFrameTransport<TSelf, in TEndpoint> : IZFrameTransport
    where TSelf : IZFrameTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<IZFrameConnection> ConnectAsync(
        TEndpoint endpoint,
        CancellationToken token = default);

    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint,
        CancellationToken token = default);
}

/// <summary>
/// A framing (0033): adapts a byte-layer connection into a frame-layer
/// connection by imposing the framing's delimiters - the frame layer is
/// protocol-related, so each framing embodies one wire dialect (the built-in
/// <see cref="ZmtpFraming"/> writes ZMTP's flag-and-length prefixes; a
/// WebSocket framing would map frames to binary messages). The adapter owns
/// the byte connection's I/O; aborting or disposing the frame connection
/// aborts and disposes the byte connection.
/// </summary>
public interface IZFraming
{
    /// <summary>Adapts a byte connection into a frame connection.</summary>
    IZFrameConnection Adapt(IZConnection connection);
}
