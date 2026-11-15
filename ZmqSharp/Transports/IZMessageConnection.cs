using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// A message-layer connection (0033): the currency is whole messages - the
/// atomic unit of ZeroMQ traffic. Reads yield complete wire units (a data
/// message, or a command - a single-frame control message); writes deliver
/// whole messages, iterating the frames internally. The frame wire grammar
/// (flag bytes, length prefixes, More chains) is an implementation detail of
/// the framing that produces the connection; the protocol above consumes
/// commands and delivers data messages.
/// </summary>
/// <remarks>
/// <para>
/// Contract: <see cref="ReadUnitAsync"/> returns null only on peer close
/// (EOF) and transfers the unit's message ownership to the caller, who
/// disposes it; <see cref="WriteMessageAsync"/> completes when the message
/// is delivered and the sender retains disposal (the runtime disposes
/// messages after sending). Command bodies are read as command units.
/// </para>
/// <para>
/// Concurrency, abort and dispose follow the byte contract (0030 section 3):
/// one reader and one writer may overlap; <see cref="Abort"/> is idempotent
/// and unblocks pending I/O without a close handshake; dispose releases
/// resources once operations finish. Message atomicity on the wire is the
/// framing's business: a message-based framing maps one message to one wire
/// unit (a WebSocket message, a datagram); a stream framing serializes the
/// frames contiguously and the receiver re-aggregates.
/// </para>
/// </remarks>
public interface IZMessageConnection : IDisposable
{
    /// <summary>
    /// Reads one complete wire unit: a data message or a command. Null on
    /// peer close (EOF). The caller owns and disposes the unit's message.
    /// </summary>
    ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default);

    /// <summary>Writes one whole data message; completion means delivered.</summary>
    ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default);

    /// <summary>Writes one command body (the establishment control currency).</summary>
    ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    /// <summary>Idempotently aborts in-flight I/O without a close handshake.</summary>
    void Abort();

    /// <summary>
    /// Whether the connection retains a message's frames after
    /// <see cref="WriteMessageAsync"/> completes. Copying connections
    /// (framings, kernel-mediated transports) return false: the wire copy
    /// consumed the bytes during the call and the sender disposes. Retaining
    /// connections (zero-copy in-proc channels, proxies) return true:
    /// ownership transfers on completion, the sender must not touch or
    /// dispose the message, and the receiving chain releases it - for those,
    /// a completed write says nothing about when the peer is done with the
    /// storage. When a write throws, ownership stays with the sender
    /// regardless.
    /// </summary>
    bool RetainsMessages { get; }
}

/// <summary>
/// A bound message-layer transport: accepts peers and reports each accepted
/// message connection via OnAccept after StartAsync runs - the
/// message-currency sibling of <see cref="IZTransport"/> (the listener
/// contracts cannot merge: their event currencies differ).
/// </summary>
public interface IZMessageTransport : IDisposable
{
    event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept;

    ValueTask StartAsync(CancellationToken token = default);
}

/// <summary>
/// Generic message-transport factory, mirroring
/// <see cref="IZTransport{TSelf,TEndpoint}"/>: ConnectAsync yields a
/// connected message connection; BindAsync yields a listening message
/// transport.
/// </summary>
public interface IZMessageTransport<TSelf, in TEndpoint> : IZMessageTransport
    where TSelf : IZMessageTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<IZMessageConnection> ConnectAsync(
        TEndpoint endpoint,
        CancellationToken token = default);

    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint,
        CancellationToken token = default);
}

/// <summary>
/// A framing (0033): adapts a byte-layer connection into a message-layer
/// connection by imposing the framing's wire dialect - the framing owns the
/// frame grammar (the built-in <see cref="ZmtpFraming"/> writes ZMTP's
/// flag-and-length prefixes and re-aggregates More chains into messages; a
/// WebSocket framing would map one message to one WS message). The adapter
/// owns the byte connection's I/O; aborting or disposing the message
/// connection aborts and disposes the byte connection.
/// </summary>
public interface IZFraming
{
    /// <summary>Adapts a byte connection into a message connection.</summary>
    IZMessageConnection Adapt(IZConnection connection);
}
