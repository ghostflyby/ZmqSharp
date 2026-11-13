using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS wire flag byte (RFC 45 / libzmq ws_protocol.hpp): 0x01 more,
/// 0x02 command. These are NOT the ZMTP 3.0 flag values (where 0x02 is the
/// long-size marker and 0x04 is command) - the byte position is shared but
/// the encoding differs, so every boundary converts explicitly (0033).
/// </summary>
internal static class ZwsWire
{
    public const byte More = 0x01;
    public const byte Command = 0x02;

    /// <summary>Logical ZMTP flags to the ZWS wire byte. Flags must be validated first.</summary>
    public static byte ToWireByte(ZmtpFrameFlags flags)
    {
        ZmtpFrameEncoder.ValidateFlags(flags);
        if ((flags & ZmtpFrameFlags.Command) != 0) return Command;
        return (flags & ZmtpFrameFlags.More) != 0 ? More : (byte)0;
    }

    /// <summary>The ZWS wire byte to logical ZMTP flags; reserved bits and the command+more combination are violations.</summary>
    public static ZmtpFrameFlags FromWireByte(byte wire)
    {
        if ((wire & ~(More | Command)) != 0)
            throw new ZeroMqProtocolException("reserved ZWS frame flag bits are set");
        if (wire == (More | Command))
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var flags = ZmtpFrameFlags.None;
        if ((wire & More) != 0) flags |= ZmtpFrameFlags.More;
        if ((wire & Command) != 0) flags |= ZmtpFrameFlags.Command;
        return flags;
    }
}

/// <summary>
/// ZWS command framing (0033): one mechanism command per WebSocket binary
/// message - flag byte (command) plus body, no length prefix. The bodies are
/// borrowed from the connection's receive buffer and stay valid until the
/// channel's next read, exactly like the ZMTP channel's scratch.
/// </summary>
internal sealed class ZwsCommandChannel(WebSocketConnection connection, long maxCommandSize) : IZCommandChannel
{
    public async ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default)
    {
        // The receive cap of maxCommandSize + 1 (flag byte) rejects an
        // oversized command with the connection's size violation before any
        // body is handed out.
        if (await connection.TryReceiveMessageAsync(maxCommandSize + 1, token) is not { } message) return null;

        if ((ZwsWire.FromWireByte(message.Span[0]) & ZmtpFrameFlags.Command) == 0)
            throw new ZeroMqProtocolException("expected a command message during handshake");

        return message[1..];
    }

    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => connection.SendFrameAsync(
            new ZmtpFrameData { Flags = ZmtpFrameFlags.Command, Body = new ReadOnlySequence<byte>(body) }, token);

    public void Dispose()
    {
        // The receive buffer is owned by the connection; nothing to release.
    }
}

/// <summary>
/// ZWS establishment (0033): the transport's HTTP upgrade already negotiated
/// the security mechanism via the subprotocol, so there is no ZMTP greeting;
/// establishment is the mechanism command sequence (for NULL: the READY
/// exchange) over the WebSocket command channel, driven by the same
/// mechanism sessions as ZMTP.
/// </summary>
internal sealed class ZwsEstablisher(
    WebSocketConnection connection,
    IZSecurityMechanism mechanism,
    ReadOnlyMemory<byte> localReadyBody,
    long maxCommandSize) : IZWireEstablisher
{
    public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
    {
        using var context = new ZMechanismContext(
            new ZwsCommandChannel(connection, maxCommandSize), localReadyBody, maxCommandSize);
        return await mechanism.CreateSession().RunAsync(context, token);
    }

    public void Dispose()
    {
        // The mechanism context and its channel were disposed when
        // EstablishAsync returned; nothing remains to release.
    }
}
