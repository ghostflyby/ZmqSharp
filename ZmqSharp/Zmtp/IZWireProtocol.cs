using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// A wire unit (0033, narrowed in 0034): one complete message plus the kind
/// that distinguishes establishment control traffic from data. The unit of
/// transport is the message; a command is exactly one frame without MORE.
/// The value carries no frame-grammar flags (MORE is per frame, LONG_SIZE is
/// byte encoding), so the invalid combinations cannot be represented.
/// </summary>
public readonly struct ZWireUnit
{
    public ZWireUnit(ZMessage message, bool isCommand = false)
    {
        if (isCommand && (message.Count != 1 || message[0].More))
            throw new ZeroMqProtocolException("a command is a single-frame message without MORE");

        Message = message;
        IsCommand = isCommand;
    }

    /// <summary>The complete message; ownership follows the connection contract.</summary>
    public ZMessage Message { get; }

    /// <summary>True when this unit is a single-frame control message.</summary>
    public bool IsCommand { get; }

    /// <summary>A data message unit.</summary>
    public static ZWireUnit Data(ZMessage message) => new(message);

    /// <summary>A command unit; the message must be exactly one frame without MORE.</summary>
    public static ZWireUnit Command(ZMessage message) => new(message, isCommand: true);
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
    /// Runs the one-shot establishment: the greeting plus the whole
    /// mechanism command exchange. The connection exposes its byte origin
    /// when it is byte-backed (the ZMTP greeting is a raw 64-byte stream
    /// construct there); a native message connection leaves it null, so the
    /// greeting travels as one command (0034). Returns the mechanism result
    /// (codec plus peer READY metadata) or null when the peer closed.
    /// </summary>
    ValueTask<ZMechanismResult?> EstablishAsync(
        IZMessageConnection connection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        CancellationToken token = default);

    /// <summary>
    /// Creates the established session (the codec comes from establishment).
    /// The decoded allocator materializes codec-decoded frames (the receive
    /// policy's zero-copy hook); without a codec the framing already
    /// materialized the final frames and the session passes messages
    /// through.
    /// </summary>
    IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue,
        MemoryPool<byte>? pool = null);
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

    /// <summary>
    /// Whether the underlying connection retains sent frames after a send
    /// completes (0033's retention contract): true means ownership
    /// transferred to the connection and the sender must not dispose; false
    /// means the wire copy consumed the bytes and the sender disposes.
    /// </summary>
    bool RetainsSentMessages { get; }
}
