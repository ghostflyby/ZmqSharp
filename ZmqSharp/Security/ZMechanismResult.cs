using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>Owned peer metadata and an optional codec whose ownership transfers to the established session.</summary>
public readonly struct ZMechanismResult(IZFrameCodec? codec, ReadOnlyMemory<byte> peerReadyBody)
{
    public IZFrameCodec? Codec { get; } = codec;
    public ReadOnlyMemory<byte> PeerReadyBody { get; } = peerReadyBody;
}
