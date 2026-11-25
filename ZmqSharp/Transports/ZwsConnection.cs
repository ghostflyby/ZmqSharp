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
internal sealed class ZwsConnection
    : IZMessageConnection, IZProtocolCarrier, IZCloseHandshake, IZHandshakeFrameLimit
{
    private readonly ClientWebSocket _webSocket;
    private readonly MemoryPool<byte> _pool;
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private long _maxFrameLength = long.MaxValue;
    private long _maxMessageLength = long.MaxValue;
    private int _maxFramesPerMessage = int.MaxValue;
    private Action? _onRejected;
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

    /// <summary>
    /// Applies the runtime's receive policy. Unlike a native connection that
    /// hands over whole frames, ZWS re-aggregates WebSocket messages and can
    /// grow a buffer, so it enforces the bounds while reading instead of
    /// relying on the post-delivery validation (0035).
    /// </summary>
    public void ConfigureReceive(in ZReceiveComposition composition)
    {
        _maxFrameLength = composition.MaxFrameLength;
        _maxMessageLength = composition.MaxMessageLength;
        _maxFramesPerMessage = composition.MaxFramesPerMessage;
        _onRejected = composition.OnRejected;
    }

    public async ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
    {
        if (_eof) return null;

        List<ZFrame>? frames = null;
        long accumulatedLength = 0;
        var frameCount = 0;
        try
        {
            while (true)
            {
                var remainingMessageLength = _maxMessageLength - accumulatedLength;
                if (await ReceiveMessageAsync(remainingMessageLength, token) is not { } wire)
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
                    var bodyLength = length - 1;
                    if (bodyLength > _maxFrameLength)
                        throw Reject(
                            ZReceiveRejectionReason.FrameTooLarge, _maxFrameLength, bodyLength);
                    if (bodyLength > _maxMessageLength - accumulatedLength)
                        throw Reject(
                            ZReceiveRejectionReason.MessageTooLarge, _maxMessageLength,
                            accumulatedLength + bodyLength);
                    if (frameCount >= _maxFramesPerMessage)
                        throw Reject(
                            ZReceiveRejectionReason.TooManyFrames, _maxFramesPerMessage,
                            frameCount + 1L);
                    accumulatedLength += bodyLength;
                    frameCount++;

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
        catch (Exception ex)
        {
            if (ex is ZeroMqProtocolException or ZReceiveRejectedException)
            {
                // RFC 6455 7.1.7 and 7.4.1: a failing endpoint should close
                // with an appropriate status - 1009 for a message too big,
                // 1002 for the other protocol violations.
                var tooBig = ex is ZReceiveRejectedException
                {
                    Rejection.Reason: ZReceiveRejectionReason.FrameTooLarge
                        or ZReceiveRejectionReason.MessageTooLarge,
                };
                await TrySendCloseAsync(
                    tooBig ? WebSocketCloseStatus.MessageTooBig : WebSocketCloseStatus.ProtocolError,
                    CancellationToken.None);
            }

            if (frames is not null)
                foreach (var frame in frames)
                    frame.Dispose();

            throw;
        }
    }

    public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            // One binary message per frame, never fragmented (libzmq rejects
            // fragmented ZWS messages).
            for (var i = 0; i < message.Count; i++)
            {
                var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                await SendFrameAsync(flags, message[i].ToSequence(), token);
            }
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async ValueTask WriteCommandAsync(
        ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            await SendFrameAsync(ZmtpFrameFlags.Command, new ReadOnlySequence<byte>(body), token);
        }
        finally
        {
            _sendGate.Release();
        }
    }

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

    /// <summary>The establishment identity shape needs one frame plus its diagnostic frame.</summary>
    int IZHandshakeFrameLimit.HandshakeFrameLimit => 2;

    ValueTask IZCloseHandshake.CloseAsync(ZCloseReason reason, CancellationToken token)
        => TrySendCloseAsync(
            reason == ZCloseReason.MessageTooBig
                ? WebSocketCloseStatus.MessageTooBig
                : WebSocketCloseStatus.ProtocolError,
            token);

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
    /// Reports a numeric-limit rejection through the runtime callback and
    /// returns the same public rejection type the byte receive guard uses, so
    /// callers observe a uniform reason/limit/actual payload.
    /// </summary>
    private ZReceiveRejectedException Reject(
        ZReceiveRejectionReason reason, long? limit, long? actual)
    {
        _onRejected?.Invoke();
        return new ZReceiveRejectedException(new ZReceiveRejection
        {
            Reason = reason,
            Limit = limit,
            Actual = actual,
        });
    }

    /// <summary>
    /// Sends the closing handshake reply (RFC 6455 5.5.1). Best effort: the
    /// peer may already be gone, the socket may be aborted, or teardown may
    /// have canceled the token; none of those is a protocol failure of its
    /// own. The send gate keeps the close frame from interleaving with a
    /// multipart message write.
    /// </summary>
    private async ValueTask TrySendCloseAsync(WebSocketCloseStatus status, CancellationToken token)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeout.CancelAfter(TimeSpan.FromSeconds(1));
            await _sendGate.WaitAsync(timeout.Token);
            try
            {
                if (_webSocket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                    await _webSocket.CloseOutputAsync(status, null, timeout.Token);
            }
            finally
            {
                _sendGate.Release();
            }
        }
        catch (Exception ex) when (ex is WebSocketException or ObjectDisposedException
                                       or InvalidOperationException or OperationCanceledException)
        {
            // Best effort only; the read side reports the actual failure.
        }
    }

    /// <summary>
    /// Receives one whole WebSocket binary message into an owned pooled
    /// buffer. The caller owns the owner; null means WebSocket close (EOF).
    /// Text messages and empty messages are ZWS protocol violations.
    /// </summary>
    private async ValueTask<(IMemoryOwner<byte> Owner, int Length)?> ReceiveMessageAsync(
        long remainingMessageLength, CancellationToken token)
    {
        var owner = _pool.Rent(4096);
        var filled = 0;
        var bufferedLimit = Math.Min(_maxFrameLength, remainingMessageLength);
        try
        {
            while (true)
            {
                if (filled == owner.Memory.Length)
                {
                    var next = owner.Memory.Length > int.MaxValue / 2
                        ? int.MaxValue
                        : owner.Memory.Length * 2;
                    if (bufferedLimit != long.MaxValue)
                    {
                        // Two spare bytes: one detects EndOfMessage exactly at
                        // the bound, the other a body over it.
                        var bounded = bufferedLimit > int.MaxValue - 2
                            ? int.MaxValue
                            : (int)bufferedLimit + 2;
                        if (next > bounded) next = bounded;
                    }

                    if (next <= filled)
                    {
                        var frameBound = bufferedLimit == _maxFrameLength;
                        throw Reject(
                            frameBound
                                ? ZReceiveRejectionReason.FrameTooLarge
                                : ZReceiveRejectionReason.MessageTooLarge,
                            frameBound ? _maxFrameLength : _maxMessageLength,
                            frameBound ? filled - 1L : null);
                    }

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
                    await TrySendCloseAsync(WebSocketCloseStatus.NormalClosure, token);
                    return null;
                }

                if (result.MessageType != WebSocketMessageType.Binary)
                    throw new ZeroMqProtocolException("ZWS requires binary WebSocket messages");

                filled += result.Count;
                if (filled > 1 && filled - 1L > _maxFrameLength)
                    throw Reject(
                        ZReceiveRejectionReason.FrameTooLarge, _maxFrameLength, filled - 1L);
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
