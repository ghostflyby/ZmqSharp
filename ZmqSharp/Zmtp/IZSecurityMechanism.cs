namespace ZmqSharp.Zmtp;

/// <summary>
/// A ZMTP security mechanism (0016 section 3): the advertised mechanism name
/// plus a factory for one per-connection handshake state machine. A socket is
/// configured with exactly one mechanism instance (0016 D1); the handshake
/// driver compares its <see cref="Name"/> against the peer's greeting
/// mechanism field - no reflection, no registry, so the seam is safe under
/// Native AOT. The mechanism runs its own command sequence and returns a
/// session connection for the traffic parser.
/// </summary>
public interface IZSecurityMechanism
{
    /// <summary>
    /// Mechanism name advertised in the greeting and matched against the
    /// peer's greeting field (e.g. "NULL", "PLAIN", "CURVE").
    /// </summary>
    string Name { get; }

    /// <summary>Creates the handshake state machine for one connection.</summary>
    IZMechanismSession CreateSession(ZMechanismRole role);
}
