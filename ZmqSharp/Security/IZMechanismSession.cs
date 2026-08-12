namespace ZmqSharp.Security;

/// <summary>
/// Per-connection mechanism handshake: runs the mechanism's command sequence
/// on the <see cref="ZMechanismContext"/> and returns the session connection
/// plus the peer's READY metadata. The session must exchange READY - the local
/// one (from <see cref="ZMechanismContext.LocalReadyBody"/>) at the
/// protocol-correct point of its sequence - and return the peer's READY
/// arguments in <see cref="ZMechanismResult.PeerReadyBody"/>, which the socket
/// layer parses for Socket-Type. A null return means the peer closed during
/// the handshake.
/// </summary>
public interface IZMechanismSession
{
    ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token = default);
}
