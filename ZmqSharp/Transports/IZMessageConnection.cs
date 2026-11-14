using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>
/// A connection whose native I/O shape is messages rather than a byte
/// stream: reads return whole messages, writes preserve message boundaries
/// (WebSocket, in-proc channels, datagram-style transports). Framing layers
/// consume <see cref="ReadMessageAsync"/> and <see cref="WriteMessageAsync"/>
/// directly instead of reconstructing boundaries from a byte view (0033).
/// </summary>
/// <remarks>
/// <para>
/// The contract mirrors the byte contract (0030 section 3) where the shapes
/// coincide: one reader and one writer may run concurrently; the connection
/// does not lock individual segments or own protocol encoding;
/// <see cref="IZConnection.Abort"/> is idempotent and requests I/O
/// termination immediately without implying pending operations finished; and
/// sequence nodes, input memories and backing arrays remain valid until a
/// write completes, including after cancellation requested an abort.
/// </para>
/// <para>
/// The inherited byte members are an optional fidelity projection: reads may
/// serve message payloads as a stream and each write may map to one message,
/// but where a faithful view is impossible - a zero-length message is
/// indistinguishable from EOF through <see cref="IZByteReader.ReadAsync"/> -
/// the byte members may throw <see cref="NotSupportedException"/>. Protocol
/// framing must use the message members.
/// </para>
/// <para>
/// Message framing still belongs to the session layer above the connection
/// (0030): this interface admits transport-native boundaries; it does not
/// move frame parsing or codecs into the transport.
/// </para>
/// </remarks>
public interface IZMessageConnection : IZConnection
{
    /// <summary>
    /// Reads one whole message into an owned buffer. The returned memory is
    /// borrowed and stays valid only until the next read on this connection
    /// (or <see cref="IDisposable.Dispose"/>); a consumer that must retain it
    /// copies it. A null result means the peer closed (EOF); an empty memory
    /// is a valid zero-length message - the two are distinct. A message
    /// longer than <paramref name="maxBytes"/> faults the connection: the
    /// transport bounds its receive buffer before any framing layer sees the
    /// bytes.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>?> ReadMessageAsync(long maxBytes, CancellationToken token = default);

    /// <summary>
    /// Sends one whole message: the segments are gathered into a single
    /// message on the wire, and completion means the entire input was
    /// consumed. Message boundaries are preserved - consecutive calls are
    /// consecutive messages; no partial-message completion exists.
    /// </summary>
    ValueTask WriteMessageAsync(ReadOnlySequence<byte> message, CancellationToken token = default);
}
