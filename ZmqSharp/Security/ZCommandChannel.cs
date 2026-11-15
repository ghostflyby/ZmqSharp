using ZmqSharp.Transports;

namespace ZmqSharp.Security;

/// <summary>
/// The framing-specific half of a mechanism handshake (0033): reads and
/// writes command bodies while the mechanism session above stays
/// framing-agnostic. Commands are the connection's establishment currency -
/// one command is one command message on any transport.
/// </summary>
internal interface IZCommandChannel : IDisposable
{
    /// <summary>Reads one command body. Null on EOF.</summary>
    ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default);

    /// <summary>Writes one command body during the exclusive handshake phase.</summary>
    ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);
}

/// <summary>
/// Commands over a message connection's unit primitives. The bodies a
/// channel returns are borrowed and stay valid until the channel's next
/// read (the previous command's message is released after the next read
/// completes).
/// </summary>
internal sealed class UnitCommandChannel(IZMessageConnection connection, long maxCommandSize) : IZCommandChannel
{
    private ZMessage? _current;

    public async ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default)
    {
        if (await connection.ReadUnitAsync(token) is not { } unit)
        {
            ReleaseCurrent();
            return null;
        }

        ReleaseCurrent();
        _current = unit.Message;
        if (!unit.IsCommand || unit.Message.Count != 1)
            throw new ZeroMqProtocolException("expected a command frame during handshake");

        var body = ZWireFrameLogic.Contiguous(unit.Message[0]);
        if (body.Length > maxCommandSize)
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");

        return body;
    }

    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => connection.WriteCommandAsync(body, token);

    public void Dispose()
    {
        ReleaseCurrent();
    }

    private void ReleaseCurrent()
    {
        _current?.Dispose();
        _current = null;
    }
}
