using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// A wire protocol over the frame layer (0033): owns establishment (greeting
/// and security-mechanism exchange) and the established session's send
/// surface and receive pump. The built-in <see cref="ZmqProtocol"/> is the
/// default; custom protocols plug at this seam (the protocol layer defines
/// the semantics of frames - command set, handshake sequence, aggregation).
/// </summary>
public interface IZWireProtocol
{
    /// <summary>
    /// Creates the one-shot establisher for a connection. The frame channel
    /// is always present; <paramref name="byteConnection"/> is the byte
    /// origin when the channel was adapted from bytes (the ZMTP greeting is
    /// a raw 64-byte stream construct there), or null for native frame
    /// connections, where the protocol defines its own frame dialect for it.
    /// </summary>
    IZWireEstablisher CreateEstablisher(
        IZFrameConnection frames, IZConnection? byteConnection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null);

    /// <summary>Creates the established session (the codec comes from establishment).</summary>
    IZWireSession CreateSession(IZFrameConnection frames, IZFrameCodec? codec, Action<Exception>? retire = null);
}

/// <summary>
/// The cold establishment a connection runs once: greeting plus the whole
/// mechanism command sequence. Returns the mechanism result (codec plus peer
/// READY metadata) or null when the peer closed during establishment.
/// </summary>
public interface IZWireEstablisher : IDisposable
{
    ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default);
}

/// <summary>
/// The established session's send surface plus its single receive pump. Sends
/// are serialized by the session; the pump streams frames to the callback
/// until EOF, a protocol violation, or cancellation.
/// </summary>
public interface IZWireSession : IDisposable
{
    /// <summary>Sends one whole message (all frames under the session's send gate).</summary>
    ValueTask SendAsync(ZMessage message, CancellationToken token = default);

    /// <summary>Sends one command body (e.g. an ERROR after a type mismatch).</summary>
    ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    /// <summary>
    /// Creates the session's single receive pump. The handler receives an
    /// owned <see cref="ZFrame"/> (More flag included) and owns its disposal
    /// or transfer; a false return pauses the pump until
    /// <see cref="IZWirePump.Resume"/>. The allocator materializes decoded
    /// frames on codec sessions (the receive policy's zero-copy hook); it is
    /// unused without a codec, where the framing already materialized the
    /// final frames.
    /// </summary>
    IZWirePump CreatePump(
        Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
        ZFrameAllocator? allocator, long maxCommandSize, long maxFrameLength);
}

/// <summary>The receive loop of one established session.</summary>
public interface IZWirePump
{
    ValueTask ParseAsync(CancellationToken token = default);

    /// <summary>Call after a streaming callback returned false to resume the loop.</summary>
    void Resume();
}
