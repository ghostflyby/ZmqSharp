using ZmqSharp.Security;

namespace ZmqSharp;

/// <summary>
/// ZMTP security configuration (0016 section 7): the single mechanism used by
/// every connection of a socket. The default is the NULL mechanism, preserving
/// the current behavior; replacing it is the replaceability gate of 0006
/// section 4. Mechanism instances are configured explicitly - never discovered
/// through reflection - so the seam is safe under Native AOT.
/// </summary>
public sealed class ZSecurityOptions
{
    /// <summary>Default configuration: the NULL mechanism.</summary>
    public static ZSecurityOptions Null { get; } = new();

    /// <summary>
    /// The mechanism for this socket; must be non-null. The mechanism is
    /// resolved at socket construction, before any connection is established.
    /// </summary>
    public IZSecurityMechanism Mechanism
    {
        get;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            field = value;
        }
    } = ZNullMechanism.Instance;
}
