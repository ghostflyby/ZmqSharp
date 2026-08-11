using System.Buffers.Binary;
using System.Text;

namespace ZmqSharp.Zmtp;

/// <summary>Builds ZMTP 3.0 command bodies using the RFC 23 short-string command-name format.</summary>
internal static class ZmtpCommands
{
    private static readonly byte[] ReadyName = [.. "READY"u8];
    private static readonly byte[] SocketTypePropertyName = [.. "Socket-Type"u8];
    private static readonly byte[] ErrorName = [.. "ERROR"u8];

    /// <summary>Builds a READY body carrying the Socket-Type metadata property.</summary>
    public static byte[] BuildReady(string socketType)
    {
        ArgumentNullException.ThrowIfNull(socketType);
        var socketTypeBytes = Encoding.ASCII.GetBytes(socketType);

        var body = new byte[
            1 + ReadyName.Length
              + 1 + SocketTypePropertyName.Length
              + sizeof(int) + socketTypeBytes.Length];

        var span = body.AsSpan();
        span[0] = (byte)ReadyName.Length;
        ReadyName.CopyTo(span[1..]);

        var offset = 1 + ReadyName.Length;
        span[offset] = (byte)SocketTypePropertyName.Length;
        offset++;
        SocketTypePropertyName.CopyTo(span[offset..]);
        offset += SocketTypePropertyName.Length;
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], socketTypeBytes.Length);
        offset += sizeof(int);
        socketTypeBytes.CopyTo(span[offset..]);

        return body;
    }

    /// <summary>Builds an ERROR body carrying a reason string.</summary>
    public static byte[] BuildError(string reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        var reasonBytes = Encoding.ASCII.GetBytes(reason);
        if (reasonBytes.Length > byte.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(reason), "ERROR reason must fit in one octet");

        var body = new byte[1 + ErrorName.Length + 1 + reasonBytes.Length];
        var span = body.AsSpan();
        span[0] = (byte)ErrorName.Length;
        ErrorName.CopyTo(span[1..]);
        var offset = 1 + ErrorName.Length;
        span[offset] = (byte)reasonBytes.Length;
        offset++;
        reasonBytes.CopyTo(span[offset..]);
        return body;
    }
}
