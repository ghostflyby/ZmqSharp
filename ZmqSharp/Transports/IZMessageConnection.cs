using System.Buffers;
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

    /// <summary>
    /// The raw byte origin when this connection is byte-backed, or null for a
    /// native message transport. The protocol uses it only for the raw ZMTP
    /// greeting, a stream construct with no message equivalent (0034).
    /// </summary>
    IZConnection? ByteOrigin => null;

    /// <summary>
    /// Applies the runtime's receive policy: the establishment bound before
    /// the handshake, then the traffic composition after it. It carries the
    /// zero-copy allocator, the encoded frame bound, the message-level limits,
    /// the message-boundary callback, and the rejection signal. A native
    /// message transport that hands over whole owned frames may ignore it
    /// (the default); one that re-aggregates frames while reading (ZWS)
    /// enforces the bounds itself (0034, 0035).
    /// </summary>
    void ConfigureReceive(in ZReceiveComposition composition) { }
}

/// <summary>
/// A framing (0033, reshaped in 0034): adapts a byte-layer connection into a
/// message-layer connection by imposing the framing's wire dialect - the
/// framing owns the frame grammar (the built-in <see cref="ZmtpFraming"/>
/// writes ZMTP's flag-and-length prefixes and re-aggregates More chains into
/// messages; a WebSocket framing would map one message to one WS message).
/// </summary>
public interface IZFraming
{
    /// <summary>
    /// Adapts a byte connection into a message connection, using
    /// <paramref name="pool"/> for scratch and owned copies. When
    /// <paramref name="ownsConnection"/> is true the view aborts and disposes
    /// the byte connection with itself; a byte connection that serves as its
    /// own message view passes false so its lifecycle stays single-owner.
    /// </summary>
    IZMessageConnection Adapt(
        IZConnection connection, MemoryPool<byte> pool, bool ownsConnection = true);
}
