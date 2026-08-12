using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// What the handshake driver receives when a mechanism session completes: the
/// connection the traffic parser runs on, plus the peer's READY metadata. The
/// session connection is the raw connection for cleartext mechanisms (NULL,
/// PLAIN) and a decrypt-on-read / encrypt-on-write wrapper for CURVE; the
/// parser and the socket layer are unchanged because they only ever see the
/// session. <see cref="PeerReadyBody"/> is an owned copy because the context's
/// scratch is reused by the next read.
/// </summary>
public readonly struct ZMechanismResult(IZConnection sessionConnection, byte[] peerReadyBody)
{
    public IZConnection SessionConnection { get; } = sessionConnection;

    /// <summary>Peer READY body (the metadata arguments); the driver parses Socket-Type from it.</summary>
    public byte[] PeerReadyBody { get; } = peerReadyBody;
}
