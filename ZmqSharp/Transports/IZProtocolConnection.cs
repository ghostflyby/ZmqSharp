using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// A connection that carries its own wire protocol instead of bare ZMTP
/// bytes (0033): the ZWS WebSocket connection, whose HTTP upgrade has already
/// negotiated the security mechanism, so establishment skips the ZMTP
/// greeting. The socket runtime detects this interface on an accepted or
/// connected <see cref="IZConnection"/> and drives the connection through
/// the returned establisher and session; connections without it run the
/// default ZMTP path unchanged. Byte-stream members inherited from
/// <see cref="IZConnection"/> remain the contract for byte protocols only -
/// message-framed protocols do their framing inside their own session.
/// </summary>
internal interface IZProtocolConnection : IZConnection
{
    /// <summary>
    /// The wire establisher: runs the mechanism command sequence (no ZMTP
    /// greeting) and yields the codec plus peer READY metadata.
    /// </summary>
    IZWireEstablisher CreateEstablisher(
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null);

    /// <summary>The established session (establishment has already produced the codec).</summary>
    IZWireSession CreateSession(IZFrameCodec? codec, Action<Exception>? retire);
}
