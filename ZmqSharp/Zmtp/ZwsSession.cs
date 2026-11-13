using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS established session (0033): sends whole messages as one binary
/// WebSocket message per ZMTP frame (flag byte + body) under the same send
/// gate discipline as <see cref="ZmtpSession"/>, optionally through the
/// mechanism's frame codec.
/// </summary>
internal sealed class ZwsSession(
    WebSocketConnection connection, IZFrameCodec? codec = null, Action<Exception>? retire = null)
    : IZWireSession
{
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly FrameSequence _sequence = new();

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            for (var i = 0; i < message.Count; i++)
            {
                var frame = new ZmtpFrameData
                {
                    Flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None,
                    Body = _sequence.FromFrame(message[i]),
                };
                await connection.SendFrameAsync(codec?.Encode(frame) ?? frame, token);
            }
        }
        catch (Exception failure)
        {
            if (retire is not null) retire(failure);
            else connection.Abort();
            throw;
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            var frame = new ZmtpFrameData { Flags = ZmtpFrameFlags.Command, Body = new ReadOnlySequence<byte>(body) };
            await connection.SendFrameAsync(codec?.Encode(frame) ?? frame, token);
        }
        catch (Exception failure)
        {
            if (retire is not null) retire(failure);
            else connection.Abort();
            throw;
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public IZWirePump CreatePump(
        Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
        ZFrameAllocator? allocator, MemoryPool<byte> pool, long maxCommandSize, long maxFrameLength)
        => new ZwsPump(connection, onFrame, allocator, pool, maxCommandSize, codec, maxFrameLength);

    public void Dispose()
    {
        try
        {
            codec?.Dispose();
        }
        finally
        {
            _sendGate.Dispose();
        }
    }
}
