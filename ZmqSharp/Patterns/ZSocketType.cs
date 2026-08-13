namespace ZmqSharp.Patterns;

/// <summary>
/// A socket type's identity (0015 section 2.1): the Socket-Type name
/// advertised in the ZMTP READY handshake plus the per-end predicate that
/// decides whether a peer's advertised type may connect. ZMTP socket-type
/// compatibility is asymmetric (REQ accepts REP, PUSH accepts PULL, PUB
/// accepts SUB), so a peer's type is never derived from the local type -
/// every socket type declares its own <see cref="AcceptsPeer"/>. The built-in
/// types keep the libzmq matrix (see <see cref="ZSocketTypes"/>); a custom
/// type interoperates only between ZmqSharp endpoints, whose peers must
/// advertise the same <see cref="Name"/> string (0015 section 2.3).
/// </summary>
public sealed class ZSocketType
{
    /// <summary>
    /// The Socket-Type advertised in READY. Must be non-empty ASCII of at most
    /// 255 characters so it round-trips through the READY encoder without
    /// silent mangling.
    /// </summary>
    public required string Name
    {
        get;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            switch (value.Length)
            {
                case 0:
                    throw new ArgumentException("socket-type name must not be empty", nameof(Name));
                case > byte.MaxValue:
                    throw new ArgumentException("socket-type name must not exceed 255 characters", nameof(Name));
            }

            foreach (var c in value)
                if (!char.IsAscii(c))
                    throw new ArgumentException(
                        "socket-type name must be ASCII (the READY Socket-Type is ASCII-encoded on the wire)",
                        nameof(Name));

            field = value;
        }
    } = "";

    /// <summary>True when a peer advertising this Socket-Type may connect to this endpoint.</summary>
    public required Func<string, bool> AcceptsPeer { get; init; }

    /// <summary>
    /// A custom socket type (0015 section 2.3): the name is advertised in
    /// READY and only a peer advertising the same name is accepted, so custom
    /// types interoperate only between ZmqSharp endpoints.
    /// </summary>
    public static ZSocketType ForCustom(string name)
    {
        return new ZSocketType
        {
            Name = name,
            AcceptsPeer = peerType => peerType == name
        };
    }
}
