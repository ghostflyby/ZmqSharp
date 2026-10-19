using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Owns message serialization and codec lifetime, while borrowing the byte connection.</summary>
internal sealed class ZmtpSession(IZConnection connection, IZFrameCodec? codec = null, Action<Exception>? retire = null) : IDisposable
{
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly ZmtpFrameEncoder _encoder = new(connection);
    private readonly FrameSequence _sequence = new();
    private ZmtpParser? _parser;
    public IZFrameCodec? Codec => codec;

    internal ZmtpParser CreateParser(
        Func<ZmtpParser, ZFrame, CancellationToken, ValueTask<bool>> handler, ZFrameAllocator? allocator,
        MemoryPool<byte> pool, long maxCommandSize, long maxFrameLength)
    {
        if (_parser is not null) throw new InvalidOperationException("a session has one receive parser");
        _parser = new ZmtpParser(connection, handler, allocator, pool, maxCommandSize, codec, maxFrameLength);
        return _parser;
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            if (codec is null) await _encoder.WriteMessageAsync(message, token);
            else
                for (var i = 0; i < message.Count; i++)
                    await _encoder.WriteFrameAsync(codec.Encode(new ZmtpFrameData
                    {
                        Flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None,
                        Body = _sequence.FromFrame(message[i])
                    }), token);
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
            await _encoder.WriteFrameAsync(codec?.Encode(frame) ?? frame, token);
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

    public async ValueTask SendFrameAsync(ReadOnlyMemory<byte> body, bool more, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            var frame = new ZmtpFrameData { Flags = more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None, Body = new ReadOnlySequence<byte>(body) };
            await _encoder.WriteFrameAsync(codec?.Encode(frame) ?? frame, token);
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

    public void Dispose()
    {
        try
        {
            _parser?.Dispose();
        }
        finally
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
}
