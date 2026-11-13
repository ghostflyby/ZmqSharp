using System.Buffers;
using ZmqSharp.Security;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The wire-agnostic established session surface the socket runtime drives
/// (0033): serialized message/command sends plus the one receive pump a
/// connection runs after establishment. ZMTP is the default implementation
/// (<see cref="ZmtpSession"/>); a connection that carries its own wire
/// protocol (ZWS over WebSocket) provides another through
/// <c>IZProtocolConnection</c>. Above this seam the runtime is framing-
/// agnostic: socket-type validation, peer phases, queues and materializers
/// all operate on frames, not bytes.
/// </summary>
internal interface IZWireSession : IDisposable
{
    /// <summary>Serialized send of one whole message (all frames under the send gate).</summary>
    ValueTask SendAsync(ZMessage message, CancellationToken token = default);

    /// <summary>Serialized send of one command body (e.g. an ERROR after a type mismatch).</summary>
    ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    /// <summary>
    /// Creates the session's single receive pump. The handler receives the
    /// pump itself so a false return can later be resumed via
    /// <see cref="IZWirePump.Resume"/>.
    /// </summary>
    IZWirePump CreatePump(
        Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
        ZFrameAllocator? allocator, MemoryPool<byte> pool, long maxCommandSize, long maxFrameLength);
}

/// <summary>
/// The receive loop of one established session: streams frames to the
/// callback until EOF, a protocol violation, or cancellation. A callback
/// returning false pauses the pump until <see cref="Resume"/> (called by the
/// socket runtime's resume surface).
/// </summary>
internal interface IZWirePump
{
    ValueTask ParseAsync(CancellationToken token = default);

    /// <summary>Call after a streaming callback returned false to resume the loop.</summary>
    void Resume();
}

/// <summary>
/// The cold establishment a connection runs once before its session exists:
/// for ZMTP the greeting plus the whole mechanism command sequence; for ZWS
/// the mechanism command sequence only (the transport negotiated the
/// mechanism in the HTTP upgrade). Returns the mechanism result (codec plus
/// peer READY body) or null when the peer closed during establishment.
/// </summary>
internal interface IZWireEstablisher : IDisposable
{
    ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default);
}
