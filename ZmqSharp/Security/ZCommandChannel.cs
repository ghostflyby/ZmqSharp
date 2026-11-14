using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// The framing-specific half of a mechanism handshake (0033): reads and
/// writes command bodies while the mechanism session above stays
/// framing-agnostic. Over the frame layer, one command is one command frame
/// - flag byte semantics included - on any transport.
/// </summary>
internal interface IZCommandChannel : IDisposable
{
    /// <summary>Reads one command body. Null on EOF.</summary>
    ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default);

    /// <summary>Writes one command body during the exclusive handshake phase.</summary>
    ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);
}

/// <summary>
/// Commands as command frames over the frame connection. The bodies a
/// channel returns are borrowed and stay valid until the channel's next
/// read (the previous command's frame is released after the next read
/// completes).
/// </summary>
internal sealed class FrameCommandChannel(IZFrameConnection frames, long maxCommandSize) : IZCommandChannel
{
    private ZWireFrame? _current;

    public async ValueTask<ReadOnlyMemory<byte>?> ReadCommandBodyAsync(CancellationToken token = default)
    {
        if (await frames.ReadFrameAsync(token) is not { } received)
        {
            ReleaseCurrent();
            return null;
        }

        ReleaseCurrent();
        _current = received;
        if ((received.Flags & ZmtpFrameFlags.Command) == 0)
            throw new ZeroMqProtocolException("expected a command frame during handshake");
        if (received.BodyLength > maxCommandSize)
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");

        return ZWireFrame.Contiguous(received.Frame);
    }

    public async ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        // The pooled copy travels with the frame (the reader releases it).
        await frames.WriteFrameAsync(ZWireFrame.Copy(ZmtpFrameFlags.Command, body.Span).Wire, token);
    }

    public void Dispose()
    {
        ReleaseCurrent();
    }

    private void ReleaseCurrent()
    {
        if (_current is { } current) current.Frame.Dispose();

        _current = null;
    }
}
