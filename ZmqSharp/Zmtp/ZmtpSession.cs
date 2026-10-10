using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Owns message serialization and codec lifetime, while borrowing the byte connection.</summary>
internal sealed class ZmtpSession(IZConnection connection, IZFrameCodec? codec = null, Action<Exception>? retire = null) : IDisposable
{
    private readonly SemaphoreSlim sendGate = new(1, 1);
    private readonly ZmtpFrameEncoder encoder = new(connection);
    private readonly FrameSequence sequence = new();
    private ZmtpParser? parser;
    public IZFrameCodec? Codec => codec;

    internal ZmtpParser CreateParser(ZFrameHandlerAsync handler, ZFrameAllocator? allocator,
        MemoryPool<byte> pool, long maxCommandSize, long maxFrameLength)
    {
        if (parser is not null) throw new InvalidOperationException("a session has one receive parser");
        parser = new ZmtpParser(connection, handler, allocator, pool, maxCommandSize, codec, maxFrameLength);
        return parser;
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        await sendGate.WaitAsync(token);
        try
        {
            if (codec is null) await encoder.WriteMessageAsync(message, token);
            else
                for (var i = 0; i < message.Count; i++)
                    await encoder.WriteFrameAsync(codec.Encode(new ZmtpFrameData
                    {
                        Flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None,
                        Body = sequence.FromFrame(message[i])
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
            sendGate.Release();
        }
    }

    public async ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        await sendGate.WaitAsync(token);
        try
        {
            var frame = new ZmtpFrameData { Flags = ZmtpFrameFlags.Command, Body = new ReadOnlySequence<byte>(body) };
            await encoder.WriteFrameAsync(codec?.Encode(frame) ?? frame, token);
        }
        catch (Exception failure)
        {
            if (retire is not null) retire(failure);
            else connection.Abort();
            throw;
        }
        finally
        {
            sendGate.Release();
        }
    }

    public async ValueTask SendFrameAsync(ReadOnlyMemory<byte> body, bool more, CancellationToken token = default)
    {
        await sendGate.WaitAsync(token);
        try
        {
            var frame = new ZmtpFrameData { Flags = more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None, Body = new ReadOnlySequence<byte>(body) };
            await encoder.WriteFrameAsync(codec?.Encode(frame) ?? frame, token);
        }
        catch (Exception failure)
        {
            if (retire is not null) retire(failure);
            else connection.Abort();
            throw;
        }
        finally
        {
            sendGate.Release();
        }
    }

    public void Dispose()
    {
        try
        {
            parser?.Dispose();
        }
        finally
        {
            try
            {
                codec?.Dispose();
            }
            finally
            {
                sendGate.Dispose();
            }
        }
    }
}
