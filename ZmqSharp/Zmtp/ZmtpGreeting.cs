using System.Text;
using ZmqSharp.Security;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Builds and validates the 64-byte ZMTP 3.0 greeting (RFC 23): signature,
/// version, the 20-byte mechanism field, and the as-server bit. The peer's
/// as-server bit is never enforced - libzmq/NetMQ write it as zero and ignore
/// it, and enforcing it would break a client connecting to a peer that does
/// not advertise the role (0016 open question).
/// </summary>
internal static class ZmtpGreeting
{
    /// <summary>Builds the local greeting for a mechanism and connection role.</summary>
    public static byte[] Build(string mechanismName, ZMechanismRole role)
    {
        ArgumentNullException.ThrowIfNull(mechanismName);
        var name = Encoding.ASCII.GetBytes(mechanismName);
        if (name.Length is 0 or > 20 || name.Length != mechanismName.Length)
            throw new ArgumentException("mechanism name must be 1-20 ASCII characters", nameof(mechanismName));

        var greeting = new byte[64];
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;
        name.CopyTo(greeting.AsSpan(12));
        greeting[32] = role == ZMechanismRole.Server ? (byte)1 : (byte)0;
        return greeting;
    }

    /// <summary>
    /// Validates a peer greeting (signature and version) and returns its
    /// mechanism name, trimmed at the NUL padding; the padding must be
    /// zero-filled and the name non-empty.
    /// </summary>
    public static string ParseMechanism(ReadOnlySpan<byte> greeting)
    {
        if (greeting[0] != 0xFF || greeting[9] != 0x7F)
            throw new ZeroMqProtocolException("invalid ZMTP greeting signature");

        if (greeting[10] < 3)
            // Greeting revision 0 = ZMTP 1.0, revision 1 = ZMTP 2.0. The whole
            // maintained ZeroMQ ecosystem is on ZMTP 3.0/3.1, so legacy peers
            // are rejected explicitly rather than negotiated down (libzmq
            // itself only keeps the legacy paths for backward compatibility).
            throw new ZeroMqProtocolException(greeting[10] switch
            {
                0 => "ZMTP 1.0 peers are not supported; only ZMTP 3.0 is implemented",
                1 => "ZMTP 2.0 peers are not supported; only ZMTP 3.0 is implemented",
                _ => "unsupported ZMTP version"
            });

        var field = greeting.Slice(12, 20);
        var nameLength = 0;
        while (nameLength < field.Length && field[nameLength] != 0) nameLength++;
        if (nameLength == 0) throw new ZeroMqProtocolException("greeting mechanism name is empty");

        for (var i = nameLength; i < field.Length; i++)
            if (field[i] != 0)
                throw new ZeroMqProtocolException("greeting mechanism name padding is not zero-filled");

        return Encoding.ASCII.GetString(field[..nameLength]);
    }
}
