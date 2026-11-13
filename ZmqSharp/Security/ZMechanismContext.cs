using System.Buffers;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// Connection-scoped wire view a mechanism session drives on (0016 section 3).
/// The context lives only for the duration of <see cref="IZMechanismSession.RunAsync"/>;
/// the driver disposes it when the session returns, so a session must not
/// retain the context or the command memories it hands out. Commands are read
/// through a framing-specific <c>IZCommandChannel</c> (ZMTP command frames on
/// the byte connection by default; ZWS binary messages over WebSocket, 0033)
/// into a reusable scratch; the returned <see cref="ZMechanismCommand"/>
/// memories are borrowed and stay valid only until the next read on this
/// context.
/// </summary>
public sealed class ZMechanismContext : IDisposable
{
    private readonly IZCommandChannel _channel;

    internal ZMechanismContext(
        IZConnection connection,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize,
        MemoryPool<byte>? pool = null)
        : this(new ZmtpCommandChannel(connection, maxCommandSize, pool), localReadyBody, maxCommandSize)
    {
    }

    internal ZMechanismContext(
        IZCommandChannel channel,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize)
    {
        _channel = channel;
        LocalReadyBody = localReadyBody;
        MaxCommandSize = maxCommandSize;
    }

    /// <summary>
    /// Local READY body built by the socket layer; the session sends it at the
    /// protocol-correct point of its command sequence (NULL: immediately;
    /// PLAIN: after WELCOME).
    /// </summary>
    public ReadOnlyMemory<byte> LocalReadyBody { get; }

    /// <summary>Command-frame size limit shared with the traffic parser (0008 Slice B).</summary>
    public long MaxCommandSize { get; }

    /// <summary>Writes one command (framing is the channel's) during the exclusive handshake phase.</summary>
    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return _channel.WriteCommandAsync(body, token);
    }

    /// <summary>
    /// Reads one command. The returned command's memories are borrowed
    /// from the channel's scratch and stay valid until the next read; a
    /// session that must retain them copies them. Returns null on EOF.
    /// </summary>
    public async ValueTask<ZMechanismCommand?> ReadCommandAsync(CancellationToken token = default)
    {
        var body = await _channel.ReadCommandBodyAsync(token);
        if (body is null) return null;

        if (!ZmtpCommandCodec.TryReadCommandName(body.Value.Span, out var name))
            throw new ZeroMqProtocolException("malformed command name");

        return new ZMechanismCommand(
            body.Value.Slice(1, name.Length),
            body.Value.Slice(1 + name.Length));
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}
