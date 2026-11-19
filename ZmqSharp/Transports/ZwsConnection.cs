using System.Buffers;
using System.Net.WebSockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// A ZWS connection over an upgraded client WebSocket (0035). The HTTP
/// upgrade negotiated the security mechanism in the subprotocol, so the
/// connection carries its own wire protocol. One WebSocket binary message is
/// one ZWS frame: a ZWS flag byte followed by the frame body, with no length
/// prefix (the WebSocket message boundary is the frame boundary). Reads
/// re-aggregate MORE chains into complete messages, exactly like a byte
/// framing; the ZWS flag values differ from ZMTP's and convert explicitly in
/// <see cref="ZwsWire"/>.
/// </summary>
internal sealed class ZwsConnection : IZMessageConnection, IZProtocolCarrier
{
    private readonly ClientWebSocket _webSocket;
    private readonly MemoryPool<byte> _pool;
    private bool _eof;
    private int _aborted;
    private int _disposed;

    internal ZwsConnection(ClientWebSocket webSocket, string subProtocol, MemoryPool<byte> pool)
    {
        _webSocket = webSocket;
        SubProtocol = subProtocol;
        _pool = pool;
    }

    /// <summary>The subprotocol the HTTP upgrade negotiated.</summary>
    internal string SubProtocol { get; }

    /// <summary>The mechanism suffix of the negotiated subprotocol, if any.</summary>
    internal string? NegotiatedMechanism
        => SubProtocol.StartsWith(ZwsProtocol.VersionPrefix, StringComparison.Ordinal)
            ? SubProtocol[ZwsProtocol.VersionPrefix.Length..]
            : null;

    /// <summary>The ZWS connection speaks its own protocol; no ZMTP greeting.</summary>
    public IZWireProtocol Protocol => ZwsProtocol.Instance;

    /// <summary>Copying connection: writes consume the frames during the call.</summary>
    public bool RetainsMessages => false;

    public async ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
    {
        if (_eof) return null;

        List<ZFrame>? frames = null;
        try
        {
            while (true)
            {
                if (await ReceiveMessageAsync(token) is not { } wire)
                {
                    // A peer close ends the stream; a partial MORE chain is
                    // released, matching the ZMTP framing's close behavior.
                    if (frames is not null)
                        foreach (var accumulated in frames)
                            accumulated.Dispose();

                    return null;
                }

                var (owner, length) = wire;
                ZFrame frame;
                bool more;
                try
                {
                    var flags = ZwsWire.FromWireByte(owner.Memory.Span[0]);
                    more = (flags & ZmtpFrameFlags.More) != 0;
                    frame = new ZFrame(new ZSegment(owner, 1, length - 1), more);
                    if ((flags & ZmtpFrameFlags.Command) != 0)
                    {
                        if (frames is { Count: > 0 })
                            throw new ZeroMqProtocolException("command frame interrupts a More chain");

                        return ZWireUnit.Command(new ZMessage(new ZSingleMessage(frame)));
                    }
                }
                catch
                {
                    owner.Dispose();
                    throw;
                }

                frames ??= [];
                frames.Add(frame);
                if (more) continue;

                var message = frames.Count == 1
                    ? new ZMessage(new ZSingleMessage(frames[0]))
                    : new ZMessage(new ZMultiMessage([.. frames]));
                frames = null;
                return ZWireUnit.Data(message);
            }
        }
        catch
        {
            if (frames is not null)
                foreach (var frame in frames)
                    frame.Dispose();

            throw;
        }
    }

    public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
    {
        // One binary message per frame, never fragmented (libzmq rejects
        // fragmented ZWS messages).
        for (var i = 0; i < message.Count; i++)
        {
            var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
            await SendFrameAsync(flags, message[i].ToSequence(), token);
        }
    }

    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => SendFrameAsync(ZmtpFrameFlags.Command, new ReadOnlySequence<byte>(body), token);

    public void Abort()
    {
        if (Interlocked.Exchange(ref _aborted, 1) != 0) return;
        _webSocket.Abort();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _webSocket.Dispose();
    }

    /// <summary>
    /// Sends one ZWS frame as a single unfragmented binary message: the wire
    /// flag byte followed by the frame body.
    /// </summary>
    private async ValueTask SendFrameAsync(
        ZmtpFrameFlags flags, ReadOnlySequence<byte> body, CancellationToken token)
    {
        var wire = ZwsWire.ToWireByte(flags);
        var total = checked(1 + (int)body.Length);
        var owner = _pool.Rent(total);
        try
        {
            var buffer = owner.Memory.Span;
            buffer[0] = wire;
            var position = 1;
            foreach (var segment in body)
            {
                segment.Span.CopyTo(buffer[position..]);
                position += segment.Length;
            }

            await _webSocket.SendAsync(owner.Memory[..total], WebSocketMessageType.Binary, true, token);
        }
        finally
        {
            owner.Dispose();
        }
    }

    /// <summary>
    /// Receives one whole WebSocket binary message into an owned pooled
    /// buffer. The caller owns the owner; null means WebSocket close (EOF).
    /// Text messages and empty messages are ZWS protocol violations.
    /// </summary>
    private async ValueTask<(IMemoryOwner<byte> Owner, int Length)?> ReceiveMessageAsync(
        CancellationToken token)
    {
        var owner = _pool.Rent(4096);
        var filled = 0;
        try
        {
            while (true)
            {
                if (filled == owner.Memory.Length)
                {
                    var next = owner.Memory.Length > int.MaxValue / 2 ? int.MaxValue : owner.Memory.Length * 2;
                    var larger = _pool.Rent(next);
                    owner.Memory[..filled].CopyTo(larger.Memory);
                    owner.Dispose();
                    owner = larger;
                }

                var result = await _webSocket.ReceiveAsync(owner.Memory[filled..], token);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    _eof = true;
                    owner.Dispose();
                    return null;
                }

                if (result.MessageType != WebSocketMessageType.Binary)
                    throw new ZeroMqProtocolException("ZWS requires binary WebSocket messages");

                filled += result.Count;
                if (result.EndOfMessage) break;
            }

            if (filled == 0) throw new ZeroMqProtocolException("empty ZWS message");

            return (owner, filled);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }
}
