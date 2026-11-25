using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// Owned peer metadata and an optional codec whose ownership transfers to the
/// established session. A READY-based establishment supplies
/// <see cref="PeerReadyBody"/>; a protocol with no mechanism supplies the
/// peer's routing identity through <see cref="PeerIdentity"/> instead, and
/// <see cref="PeerReadyBody"/> stays empty.
/// </summary>
public readonly struct ZMechanismResult
{
    public ZMechanismResult(IZFrameCodec? codec, ReadOnlyMemory<byte> peerReadyBody)
        : this(codec, peerReadyBody, null)
    {
    }

    private ZMechanismResult(
        IZFrameCodec? codec, ReadOnlyMemory<byte> peerReadyBody, ReadOnlyMemory<byte>? peerIdentity)
    {
        Codec = codec;
        PeerReadyBody = peerReadyBody;
        PeerIdentity = peerIdentity;
    }

    public IZFrameCodec? Codec { get; }

    public ReadOnlyMemory<byte> PeerReadyBody { get; }

    /// <summary>
    /// The peer's routing identity when establishment had no READY metadata
    /// (the ZWS no-mechanism profile, 0035); null when
    /// <see cref="PeerReadyBody"/> carries READY metadata. An empty value
    /// means the peer advertised no identity.
    /// </summary>
    public ReadOnlyMemory<byte>? PeerIdentity { get; }

    /// <summary>
    /// Creates a result for an establishment that exchanges an identity
    /// message instead of READY metadata. The caller owns
    /// <paramref name="peerIdentity"/>'s storage; the runtime consumes it
    /// during establishment and any long-lived owner copies it.
    /// </summary>
    public static ZMechanismResult WithoutReady(
        IZFrameCodec? codec, ReadOnlyMemory<byte> peerIdentity)
        => new(codec, default, peerIdentity);
}
