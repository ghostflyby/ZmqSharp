using System.Buffers.Binary;
using System.Text;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Shared ZMTP command wire-format helpers (RFC 23 short-string command names,
/// READY metadata properties, ERROR reasons). The handshake driver, the
/// traffic parser, and mechanism sessions all use this codec; it is public so
/// custom mechanisms (e.g. PLAIN's HELLO) encode and decode command bodies
/// with the same rules the library enforces - a mechanism must never re-implement
/// the wire format (0016 section 3).
/// </summary>
public static class ZmtpCommandCodec
{
    /// <summary>
    /// Reads the short-string command name from the start of a body (one-byte
    /// length prefix, alpha-only name). Returns false on a malformed name.
    /// </summary>
    public static bool TryReadCommandName(ReadOnlySpan<byte> body, out ReadOnlySpan<byte> name)
    {
        if (body.IsEmpty || body[0] == 0)
        {
            name = default;
            return false;
        }

        var nameLength = body[0];
        if (body.Length < nameLength + 1)
        {
            name = default;
            return false;
        }

        var candidate = body.Slice(1, nameLength);
        foreach (var c in candidate)
        {
            var isAlpha = (c >= (byte)'A' && c <= (byte)'Z') || (c >= (byte)'a' && c <= (byte)'z');
            if (!isAlpha)
            {
                name = default;
                return false;
            }
        }

        name = candidate;
        return true;
    }

    /// <summary>
    /// Parses the peer's READY metadata arguments and returns the Socket-Type
    /// property value; a missing or empty Socket-Type is a protocol error
    /// (RFC 23 / 0015 section 2.4). The value is not validated against the
    /// built-in names: custom socket types interoperate between ZmqSharp
    /// endpoints (0015 section 2.3), so an unknown name is accepted here and
    /// decided by the local socket's <see cref="ZSocketType.AcceptsPeer"/>
    /// predicate at connection time.
    /// </summary>
    public static string ParseReadySocketType(ReadOnlySpan<byte> metadata)
    {
        var properties = ParseMetadata(metadata);
        if (!properties.TryGetValue("Socket-Type", out var peerType) || peerType.Length == 0)
            throw new ZeroMqProtocolException("READY is missing a valid Socket-Type property");

        return peerType;
    }

    /// <summary>
    /// Parses the peer's ERROR command arguments and returns the reason string;
    /// a malformed ERROR body is a protocol error. The reason is free-form
    /// printable ASCII including spaces - libzmq's rejection reasons (e.g.
    /// "Invalid username or password") contain spaces.
    /// </summary>
    public static string ParseErrorReason(ReadOnlySpan<byte> body)
    {
        if (body.IsEmpty) throw new ZeroMqProtocolException("malformed ERROR command");

        var reasonLength = body[0];
        if (body.Length != 1 + reasonLength)
            throw new ZeroMqProtocolException("ERROR reason length does not match the command body");

        foreach (var c in body[1..])
            if (c is < 0x20 or > 0x7E)
                throw new ZeroMqProtocolException("ERROR reason contains a non-visible character");

        return Encoding.UTF8.GetString(body[1..]);
    }

    /// <summary>
    /// Parses a metadata property sequence (the READY metadata format: 1-byte
    /// name length, name, 4-byte big-endian value length, value) into a
    /// case-insensitive property dictionary. Malformed properties, duplicate
    /// names, and out-of-range lengths are protocol errors.
    /// </summary>
    public static Dictionary<string, string> ParseMetadata(ReadOnlySpan<byte> metadata)
    {
        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var offset = 0;
        while (offset < metadata.Length)
        {
            var nameLength = metadata[offset];
            offset++;
            if (nameLength == 0) throw new ZeroMqProtocolException("metadata property name is empty");

            if (metadata.Length - offset < nameLength)
                throw new ZeroMqProtocolException("metadata property name exceeds command body");

            var name = metadata.Slice(offset, nameLength);
            foreach (var c in name)
                if (!IsMetadataNameChar(c))
                    throw new ZeroMqProtocolException("metadata property name contains an invalid character");

            offset += nameLength;
            if (metadata.Length - offset < sizeof(int))
                throw new ZeroMqProtocolException("metadata property value length is truncated");

            var valueLength = BinaryPrimitives.ReadInt32BigEndian(metadata[offset..]);
            offset += sizeof(int);
            if (valueLength < 0 || valueLength > metadata.Length - offset)
                throw new ZeroMqProtocolException("metadata property value exceeds command body");

            var nameString = Encoding.ASCII.GetString(name);
            var value = Encoding.UTF8.GetString(metadata.Slice(offset, valueLength));
            offset += valueLength;
            if (!properties.TryAdd(nameString, value))
                throw new ZeroMqProtocolException($"duplicate metadata property '{nameString}'");
        }

        return properties;
    }

    /// <summary>Exact byte length a metadata property occupies: 1 + name + 4 + value.</summary>
    public static int MetadataPropertyLength(int nameLength, int valueLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(nameLength);
        ArgumentOutOfRangeException.ThrowIfNegative(valueLength);
        if (nameLength > byte.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(nameLength), "metadata property name must fit in one octet");

        return 1 + nameLength + sizeof(int) + valueLength;
    }

    /// <summary>
    /// Writes one metadata property (the READY metadata format used by PLAIN's
    /// HELLO Username/Password) into <paramref name="destination"/> and returns
    /// the number of bytes written. The property name must be non-empty, ASCII
    /// metadata-name characters only, and fit in one octet.
    /// </summary>
    public static int WriteMetadataProperty(
        Span<byte> destination,
        ReadOnlySpan<byte> name,
        ReadOnlySpan<byte> value)
    {
        ArgumentException.ThrowIfNullOrEmpty(nameof(name));
        foreach (var c in name)
            if (!IsMetadataNameChar(c))
                throw new ArgumentException("metadata property name contains an invalid character", nameof(name));

        if (name.Length > byte.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(name), "metadata property name must fit in one octet");

        var required = MetadataPropertyLength(name.Length, value.Length);
        if (destination.Length < required)
            throw new ArgumentException("destination is too small for the metadata property", nameof(destination));

        destination[0] = (byte)name.Length;
        name.CopyTo(destination[1..]);
        BinaryPrimitives.WriteInt32BigEndian(destination[(1 + name.Length)..], value.Length);
        value.CopyTo(destination[(1 + name.Length + sizeof(int))..]);
        return required;
    }

    private static bool IsMetadataNameChar(byte c)
    {
        return char.IsAsciiLetterOrDigit((char)c)
               || c == (byte)'-'
               || c == (byte)'_'
               || c == (byte)'.'
               || c == (byte)'+';
    }
}
