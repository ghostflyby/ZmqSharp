using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS wire protocol (0035): the WebSocket upgrade's subprotocol already
/// selected the security mechanism, so establishment is the mechanism command
/// exchange only - there is no ZMTP greeting. The bare <c>ZWS2.0</c> profile
/// has no mechanism at all and exchanges the routing identity instead of
/// READY. Traffic is one ZWS frame per binary WebSocket message, handled by the
/// native <see cref="ZwsConnection"/>; the established session is the shared
/// message session.
/// </summary>
internal sealed class ZwsProtocol : IZWireProtocol
{
    /// <summary>The RFC 45 version-and-mechanism prefix (the proposal uses '+').</summary>
    internal const string VersionPrefix = "ZWS2.0+";

    /// <summary>The NULL-mechanism subprotocol (ZWS2.0+NULL).</summary>
    internal const string NullSubProtocol = VersionPrefix + "NULL";

    /// <summary>
    /// The RFC 45 no-mechanism profile (bare <c>ZWS2.0</c>): the mandatory
    /// fallback for peers without ZMTP 3.0 mechanisms. The first message each
    /// way is the routing identity; there is no greeting or READY exchange.
    /// </summary>
    internal const string NoMechanismSubProtocol = "ZWS2.0";

    public static ZwsProtocol Instance { get; } = new();

    private ZwsProtocol()
    {
    }

    public async ValueTask<ZMechanismResult?> EstablishAsync(
        IZMessageConnection connection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        CancellationToken token = default)
    {
        if (connection is not ZwsConnection zws)
            throw new NotSupportedException("the ZWS protocol requires a ZWS WebSocket connection");

        if (string.Equals(zws.SubProtocol, NoMechanismSubProtocol, StringComparison.Ordinal))
        {
            // Bare ZWS2.0 is the RFC 45 no-mechanism profile; it is only
            // meaningful for a NULL-configured socket (the ZMTP mechanism is
            // not used at all on this path).
            if (!string.Equals(mechanism.Name, ZNullMechanism.Instance.Name, StringComparison.Ordinal))
                throw new NotSupportedException(
                    $"the WebSocket subprotocol '{zws.SubProtocol}' selects the no-mechanism profile; the socket is configured for the '{mechanism.Name}' mechanism");

            return await EstablishNoMechanismAsync(connection, localReadyBody, token);
        }

        var negotiated = zws.NegotiatedMechanism;
        if (!string.Equals(negotiated, mechanism.Name, StringComparison.Ordinal))
            throw new NotSupportedException(
                $"the WebSocket subprotocol '{zws.SubProtocol}' negotiates the '{negotiated ?? "<none>"}' mechanism; the socket is configured for '{mechanism.Name}'");

        // The upgrade negotiated the mechanism, so the greeting is skipped;
        // NULL runs its READY exchange through the same mechanism layer. The
        // context owns the final peer command's message, so the using scope
        // must cover the whole asynchronous exchange (not just the call that
        // starts it).
        using var context = new ZMechanismContext(
            new UnitCommandChannel(connection, maxCommandSize), localReadyBody, maxCommandSize);
        return await mechanism.CreateSession().RunAsync(context, token);
    }

    /// <summary>
    /// The RFC 45 no-mechanism handshake: each peer sends its routing identity
    /// as the first data message and reads the peer's. No READY metadata
    /// exists, so the peer's socket type cannot be validated; the runtime
    /// skips that check for a <see cref="ZMechanismResult.WithoutReady"/>
    /// result.
    /// </summary>
    private static async ValueTask<ZMechanismResult?> EstablishNoMechanismAsync(
        IZMessageConnection connection, ReadOnlyMemory<byte> localReadyBody, CancellationToken token)
    {
        // The local READY body is the full command body (name plus metadata).
        // It is the only place the configured routing identity reaches the
        // protocol; skip the name to read the metadata, then extract the
        // identity. It is empty for types that do not advertise one, which is
        // still a valid (empty) identity frame.
        if (!ZmtpCommandCodec.TryReadCommandName(localReadyBody.Span, out var readyName) ||
            !readyName.SequenceEqual("READY"u8))
            throw new ZeroMqProtocolException("the local READY body is malformed");

        var localMetadata = localReadyBody[(1 + readyName.Length)..];
        var localIdentity = ZmtpCommandCodec.ParseReadyIdentity(localMetadata.Span)
                            ?? ReadOnlyMemory<byte>.Empty;

        using (var outgoing = ZMessage.Copy(localIdentity))
            await connection.WriteMessageAsync(outgoing, token);

        if (await connection.ReadUnitAsync(token) is not { } unit) return null;

        using (unit.Message)
        {
            if (unit.IsCommand)
                throw new ZeroMqProtocolException(
                    "the ZWS2.0 no-mechanism handshake requires an identity message, not a command");
            if (unit.Message.Count != 1 || unit.Message[0].More)
                throw new ZeroMqProtocolException(
                    "the ZWS2.0 no-mechanism identity must be a single frame");

            var peerIdentity = unit.Message[0].ToSequence().ToArray();
            return ZMechanismResult.WithoutReady(codec: null, peerIdentity);
        }
    }

    public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue,
        MemoryPool<byte>? pool = null)
        => new ZMessageSession(connection, codec, retire, decodedAllocator, maxCommandSize, maxFrameLength,
            pool ?? MemoryPool<byte>.Shared);
}
