using System.Buffers;
using System.Net.WebSockets;
using ZmqSharp.Security;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// A ZWS connection over an upgraded WebSocket (0033). The transport
/// performed the HTTP upgrade and subprotocol negotiation, so the connection
/// carries its own wire protocol: ZWS frames are single binary WebSocket
/// messages, one ZMTP frame each, with the ZMTP flag byte as the first
/// payload byte and no length prefix. The byte-stream members inherited from
/// <see cref="IZConnection"/> satisfy the interface contract (reads walk
/// message payloads, each write is one message), but the framing layers use
/// the message-preserving <see cref="TryReceiveMessageAsync"/> and
/// <see cref="SendFrameAsync"/> instead, because ZWS frame boundaries are
/// WebSocket message boundaries. Outgoing messages are never fragmented
/// (libzmq rejects fragmented messages); incoming fragmentation cannot be
/// observed through the BCL WebSocket and is accepted byte-equivalently.
/// </summary>
internal sealed class WebSocketConnection : IZConnection, IZProtocolConnection
{
    private readonly WebSocket _webSocket;
    private readonly string? _subProtocol;
    private readonly MemoryPool<byte> _pool;

    // The current received message: payload borrowed from the pool owner,
    // valid until the next receive. _messagePos tracks byte-view consumption;
    // the ZWS framing layers consume whole messages and leave it at length.
    private IMemoryOwner<byte>? _messageOwner;
    private Memory<byte> _message;
    private int _messagePos;
    private bool _eof;

    internal WebSocketConnection(WebSocket webSocket, string? subProtocol, MemoryPool<byte>? pool = null)
    {
        _webSocket = webSocket;
        _subProtocol = subProtocol;
        _pool = pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>
    /// The subprotocol the HTTP upgrade negotiated. Carried here rather than
    /// on the BCL WebSocket, whose subprotocol token grammar rejects the '/'
    /// in "ZWS2.0/NULL".
    /// </summary>
    internal string? SubProtocol => _subProtocol;

    // ---- IZByteReader: the message payloads as a byte stream ----

    public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
    {
        while (true)
        {
            if (_messagePos < _message.Length)
            {
                var count = Math.Min(destination.Length, _message.Length - _messagePos);
                _message.Slice(_messagePos, count).CopyTo(destination[..count]);
                _messagePos += count;
                return count;
            }

            if (_eof) return 0;

            if (await TryReceiveMessageAsync(int.MaxValue, token) is null) return 0;
        }
    }

    // ---- IZByteWriter: each write call is one binary message ----

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => _webSocket.SendAsync(bytes, WebSocketMessageType.Binary, true, token);

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        if (bytes.IsSingleSegment)
        {
            await WriteAsync(bytes.First, token);
            return;
        }

        var buffer = ArrayPool<byte>.Shared.Rent(checked((int)bytes.Length));
        try
        {
            bytes.CopyTo(buffer);
            await WriteAsync(buffer.AsMemory(0, (int)bytes.Length), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Abort() => _webSocket.Abort();

    public void Dispose() => _webSocket.Dispose();

    // ---- IZProtocolConnection: the ZWS wire protocol ----

    public IZWireEstablisher CreateEstablisher(
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null)
    {
        // The HTTP upgrade negotiated exactly one mechanism via the
        // subprotocol; a socket configured differently cannot run over this
        // connection (0033: this transport offers ZWS2.0/NULL only).
        if (!string.Equals(SubProtocol, WsUpgrade.NullSubprotocol, StringComparison.Ordinal) ||
            !string.Equals(mechanism.Name, "NULL", StringComparison.Ordinal))
            throw new NotSupportedException(
                $"the WebSocket transport negotiates the NULL mechanism via the '{WsUpgrade.NullSubprotocol}' subprotocol; mechanism '{mechanism.Name}' over '{SubProtocol}' is not supported");

        return new ZwsEstablisher(this, mechanism, localReadyBody, maxCommandSize);
    }

    public IZWireSession CreateSession(IZFrameCodec? codec, Action<Exception>? retire)
        => new ZwsSession(this, codec, retire);

    // ---- Message-preserving primitives for the ZWS framing layers ----

    /// <summary>
    /// Receives one whole WebSocket message into an owned buffer. The payload
    /// is borrowed and stays valid until the next receive on this connection;
    /// null means WebSocket close (surfaced as EOF). Text messages, empty
    /// messages, and messages beyond <paramref name="maxBytes"/> are ZWS
    /// protocol violations.
    /// </summary>
    internal async ValueTask<ReadOnlyMemory<byte>?> TryReceiveMessageAsync(long maxBytes, CancellationToken token)
    {
        if (_eof) return null;

        // The framing layers consume whole messages; the byte view is the
        // only partial consumer, and one protocol runs per connection, so a
        // new receive simply supersedes whatever the byte view had left.
        _messagePos = _message.Length;

        var filled = 0;
        while (true)
        {
            if (filled == _message.Length)
            {
                if (filled >= maxBytes)
                    throw new ZeroMqProtocolException($"WebSocket message exceeds the maximum of {maxBytes} bytes");

                EnsureMessageCapacity(checked((int)Math.Min(
                    Math.Max(4096, (long)_message.Length * 2), Math.Min(maxBytes + 1, int.MaxValue))));
            }

            var result = await _webSocket.ReceiveAsync(_message[filled..], token);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                _eof = true;
                _message = default;
                return null;
            }

            if (result.MessageType != WebSocketMessageType.Binary)
                throw new ZeroMqProtocolException("ZWS requires binary WebSocket messages");

            filled += result.Count;
            if (result.EndOfMessage) break;
        }

        if (filled == 0) throw new ZeroMqProtocolException("empty ZWS message");
        if (filled > maxBytes)
            throw new ZeroMqProtocolException($"WebSocket message exceeds the maximum of {maxBytes} bytes");

        _messagePos = 0;
        return _message[..filled];
    }

    /// <summary>
    /// Sends one ZWS frame: a single unfragmented binary message whose
    /// payload is the ZWS wire flag byte (RFC 45: 0x01 more, 0x02 command -
    /// not the ZMTP values) followed by the frame body. The body segments
    /// are copied into one contiguous buffer because a WebSocket message
    /// must be sent in a single call here (no fragmentation).
    /// </summary>
    internal async ValueTask SendFrameAsync(ZmtpFrameData frame, CancellationToken token = default)
    {
        var total = checked(1 + (int)frame.Body.Length);
        var buffer = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            buffer[0] = ZwsWire.ToWireByte(frame.Flags);
            var position = 1;
            foreach (var segment in frame.Body)
            {
                segment.Span.CopyTo(buffer.AsSpan(position, segment.Length));
                position += segment.Length;
            }

            await _webSocket.SendAsync(buffer.AsMemory(0, total), WebSocketMessageType.Binary, true, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void EnsureMessageCapacity(int required)
    {
        if (_message.Length >= required && _messageOwner is not null) return;

        var owner = _pool.Rent(required);
        // No copy: a growth only happens while a fresh message is being
        // accumulated; the previous payload is already fully consumed.
        _messageOwner?.Dispose();
        _messageOwner = owner;
        _message = owner.Memory;
    }
}
