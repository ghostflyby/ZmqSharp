using System.Buffers;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// Connection-scoped wire view a mechanism session drives on (0016 section 3).
/// The context lives only for the duration of <see cref="IZMechanismSession.RunAsync"/>;
/// the driver disposes it when the session returns, so a session must not
/// retain the context or the command memories it hands out. Commands travel
/// through a framing-specific command channel (0033); the returned
/// <see cref="ZMechanismCommand"/> memories are borrowed and stay valid only
/// until the next read on this context.
/// </summary>
public sealed class ZMechanismContext : IDisposable
{
    private readonly IZCommandChannel _channel;

    internal ZMechanismContext(
        IZConnection connection,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize,
        MemoryPool<byte>? pool = null)
        : this(new UnitCommandChannel(ZmtpFraming.Instance.Adapt(connection), maxCommandSize),
            localReadyBody, maxCommandSize)
    {
        // The public constructor keeps its byte-connection shape: ZMTP
        // command framing over the byte origin (0033).
        _ = pool;
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

    /// <summary>
    /// Command-frame size limit shared with the traffic parser (0008 Slice B).
    /// Public contract: a custom mechanism reads it to size or validate the
    /// commands it exchanges, so the in-assembly write-only shape is intended.
    /// </summary>
    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    public long MaxCommandSize { get; }

    /// <summary>Writes one command (framing is the channel's) during the exclusive handshake phase.</summary>
    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return _channel.WriteCommandAsync(body, token);
    }

    /// <summary>
    /// Reads one command frame. The returned command's memories are borrowed
    /// from the channel and stay valid until the next read; a
    /// session that must retain them copies them. Returns null on EOF.
    /// </summary>
    public async ValueTask<ZMechanismCommand?> ReadCommandAsync(CancellationToken token = default)
    {
        if (await _channel.ReadCommandBodyAsync(token) is not { } body) return null;

        if (!ZmtpCommandCodec.TryReadCommandName(body.Span, out var name))
            throw new ZeroMqProtocolException("malformed command name");

        return new ZMechanismCommand(
            body.Slice(1, name.Length),
            body.Slice(1 + name.Length));
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}
