using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS wire protocol (0035): the WebSocket upgrade's subprotocol already
/// selected the security mechanism, so establishment is the mechanism command
/// exchange only - there is no ZMTP greeting. Traffic is one ZWS frame per
/// binary WebSocket message, handled by the native <see cref="ZwsConnection"/>;
/// the established session is the shared message session.
/// </summary>
internal sealed class ZwsProtocol : IZWireProtocol
{
    /// <summary>The RFC 45 version-and-mechanism prefix (the pending revision uses '+').</summary>
    internal const string VersionPrefix = "ZWS2.0+";

    /// <summary>The only mechanism negotiated by this client: NULL security.</summary>
    internal const string NullSubProtocol = VersionPrefix + "NULL";

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

    public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue,
        MemoryPool<byte>? pool = null)
        => new ZMessageSession(connection, codec, retire, decodedAllocator, maxCommandSize, maxFrameLength,
            pool ?? MemoryPool<byte>.Shared);
}
