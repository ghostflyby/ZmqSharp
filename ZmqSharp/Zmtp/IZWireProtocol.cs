using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// A wire unit (0033): one complete message plus the protocol flag bits that
/// ride it on the wire - the command bit distinguishes establishment control
/// traffic from data. The unit of transport is the message; a command is a
/// single-frame control message.
/// </summary>
public readonly struct ZWireUnit
{
    public ZWireUnit(ZmtpFrameFlags flags, ZMessage message)
    {
        ZWireFrameLogic.Validate(flags & ~ZmtpFrameFlags.Command);
        Flags = flags;
        Message = message;
    }

    /// <summary>The wire flags: command (establishment) or none/more (data).</summary>
    public ZmtpFrameFlags Flags { get; }

    /// <summary>The complete message; ownership follows the connection contract.</summary>
    public ZMessage Message { get; }

    /// <summary>True when this is a single-frame control message.</summary>
    public bool IsCommand => (Flags & ZmtpFrameFlags.Command) != 0;
}

/// <summary>
/// A wire protocol over the message layer (0033): owns establishment
/// (greeting and security-mechanism exchange, driven on the connection's
/// command primitives) and the established session's send surface and
/// message reads. The built-in <see cref="ZmqProtocol"/> is the default;
/// custom protocols plug at this seam - the protocol defines the semantics
/// of wire units (command set, handshake sequence, aggregation), the framing
/// defines their wire encoding.
/// </summary>
public interface IZWireProtocol
{
    /// <summary>
    /// Creates the one-shot establisher for a connection. The message
    /// connection is always present; <paramref name="byteConnection"/> is
    /// the byte origin when the connection was adapted from bytes (the ZMTP
    /// greeting is a raw 64-byte stream construct there), or null for native
    /// message connections, where the greeting travels as one command.
    /// </summary>
    IZWireEstablisher CreateEstablisher(
        IZMessageConnection connection, IZConnection? byteConnection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null);

    /// <summary>
    /// Creates the established session (the codec comes from establishment).
    /// The decoded allocator materializes codec-decoded frames (the receive
    /// policy's zero-copy hook); without a codec the framing already
    /// materialized the final frames and the session passes messages
    /// through.
    /// </summary>
    IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue);
}

/// <summary>
/// The cold establishment a connection runs once: greeting plus the whole
/// mechanism command exchange. Returns the mechanism result (codec plus peer
/// READY metadata) or null when the peer closed during establishment.
/// </summary>
public interface IZWireEstablisher : IDisposable
{
    ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default);
}

/// <summary>
/// The established session's send surface plus message reads. Sends are
/// serialized by the session; <see cref="ReadMessageAsync"/> yields complete
/// data messages with traffic-phase commands consumed internally.
/// </summary>
public interface IZWireSession : IDisposable
{
    /// <summary>Sends one whole message (codec applied per frame under the send gate).</summary>
    ValueTask SendAsync(ZMessage message, CancellationToken token = default);

    /// <summary>Sends one command body (e.g. an ERROR after a type mismatch).</summary>
    ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    /// <summary>
    /// Reads one complete data message; traffic-phase commands are consumed
    /// internally (ERROR faults). Null on peer close (EOF). The caller owns
    /// (and disposes) the returned message.
    /// </summary>
    ValueTask<ZMessage?> ReadMessageAsync(CancellationToken token = default);
}
