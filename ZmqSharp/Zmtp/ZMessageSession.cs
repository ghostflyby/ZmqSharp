using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The established message session shared by the built-in protocols (0033):
/// serialized sends under one gate, per-frame codec application, and message
/// reads that consume traffic commands internally. It operates on the message
/// connection alone, so a byte framing and a native message connection run
/// the same session logic.
/// </summary>
internal sealed class ZMessageSession(
    IZMessageConnection connection, IZFrameCodec? codec, Action<Exception>? retire,
    ZFrameAllocator? decodedAllocator, long maxCommandSize, long maxFrameLength,
    MemoryPool<byte> pool) : IZWireSession
{
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly FrameSequence _logical = new();

    // Codec staging reused for the connection's lifetime (single writer
    // behind the send gate, single reader in the receive loop): growing
    // ArrayPool buffers serve every frame with no steady-state GC
    // allocation (0033).
    private byte[] _sendBuffer = [];
    private readonly List<byte[]> _readStaging = [];
    private readonly List<ZFrame> _decodedFrames = [];

    // A codec encodes into a NEW message: the caller's input is never
    // handed to the connection, so ownership of the input always stays
    // with the sender. Only the no-codec path transfers it.
    public bool RetainsSentMessages => codec is null && connection.RetainsMessages;

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        await _sendGate.WaitAsync(token);
        try
        {
            if (codec is not { } encoder)
            {
                await connection.WriteMessageAsync(message, token);
                return;
            }

            if (ZStagedWrites.TryGet(connection, out var streaming))
            {
                // Built-in fast path: each frame is encoded into the
                // reused send buffer and streamed as one logical write -
                // the buffer is free again the moment the write completes
                // (the gate serializes sends), so the steady state
                // allocates nothing (0033).
                for (var i = 0; i < message.Count; i++)
                {
                    var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                    var encoded = encoder.Encode(new ZmtpFrameData
                    {
                        Flags = flags,
                        Body = _logical.FromFrame(message[i]),
                    });
                    ZWireFrameLogic.Validate(encoded.Flags);
                    var staged = StageEncoded(encoded);
                    await streaming.WriteStagedFrameAsync(encoded.Flags, staged, token);
                }

                return;
            }

            // Assembled fallback (a custom or native message connection):
            // each frame stages in its OWN owner-backed buffer - staging
            // all frames over one reused buffer would alias them into
            // the last frame's bytes (0033 review). The session owns the
            // assembled message: a copying connection consumes it during
            // the write and the session releases it here; a retaining
            // connection takes ownership and its receiving chain disposes
            // it. A throw never transfers ownership, so failures dispose.
            var assembledMessage = EncodeMessage(encoder, message);
            try
            {
                await connection.WriteMessageAsync(assembledMessage, token);
                if (!connection.RetainsMessages) assembledMessage.Dispose();
            }
            catch
            {
                assembledMessage.Dispose();
                throw;
            }
        }
        catch (Exception failure)
        {
            await FailAsync(failure);
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
            if (codec is null)
            {
                await connection.WriteCommandAsync(body, token);
                return;
            }

            if (codec is not { } commandEncoder)
            {
                await connection.WriteCommandAsync(body, token);
                return;
            }

            var encoded = commandEncoder.Encode(new ZmtpFrameData
            {
                Flags = ZmtpFrameFlags.Command,
                Body = new ReadOnlySequence<byte>(body),
            });
            ZWireFrameLogic.Validate(encoded.Flags);
            var staged = StageEncoded(encoded);
            await connection.WriteCommandAsync(ZWireFrameLogic.Contiguous(staged), token);
        }
        catch (Exception failure)
        {
            await FailAsync(failure);
            throw;
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async ValueTask<ZMessage?> ReadMessageAsync(CancellationToken token = default)
    {
        while (true)
        {
            if (await connection.ReadUnitAsync(token) is not { } unit)
            {
                ReleaseStaging(_readStaging);
                return null;
            }

            if (unit.IsCommand)
            {
                try
                {
                    using (unit.Message)
                    {
                        if (ZWireFrameLogic.ByteLength(unit.Message[0]) > maxCommandSize)
                            throw new ZeroMqProtocolException(
                                $"command frame exceeds maximum size of {maxCommandSize} bytes");

                        var body = codec is { } decoder ? DecodeBody(decoder, unit.Message)
                            : ZWireFrameLogic.Contiguous(unit.Message[0]);
                        CheckCommand(body.Span);
                    }
                }
                catch (Exception failure) when (failure is ZeroMqProtocolException or ZReceiveRejectedException)
                {
                    await CloseFaultAsync(failure);
                    throw;
                }

                // The staged command body was consumed by CheckCommand
                // inside this iteration; releasing here bounds a peer
                // driving an endless command stream (0033 review).
                ReleaseStaging(_readStaging);
                continue;
            }

            if (codec is not { } sessionCodec) return unit.Message;

            // Codec sessions decode each frame of the aggregated
            // (encoded) message into fresh frames and rebuild; logical
            // limits apply after authentication (0008). Without a
            // decoded allocator the staged frames are valid until the
            // NEXT read on this session (documented window); the runtime
            // always supplies an allocator (the materializer's, or its
            // pooled fallback), which owns its frames outright.
            ReleaseStaging(_readStaging);
            _decodedFrames.Clear();
            var decodedFrames = _decodedFrames;
            try
            {
                for (var i = 0; i < unit.Message.Count; i++)
                {
                    var wireFlags = i < unit.Message.Count - 1
                        ? ZmtpFrameFlags.More
                        : ZmtpFrameFlags.None;
                    var decoded = sessionCodec.Decode(new ZmtpFrameData
                    {
                        Flags = wireFlags,
                        Body = unit.Message[i].ToSequence(),
                    });
                    ZWireFrameLogic.Validate(decoded.Flags);
                    var length = checked((int)decoded.Body.Length);

                    if (decodedAllocator is { } materialize)
                    {
                        var frame = materialize(length, (decoded.Flags & ZmtpFrameFlags.More) != 0);
                        try
                        {
                            var remaining = decoded.Body;
                            foreach (var segment in frame)
                            {
                                remaining.Slice(0, segment.Memory.Length).CopyTo(segment.Writable.Span);
                                remaining = remaining.Slice(segment.Memory.Length);
                            }

                            decodedFrames.Add(frame);
                        }
                        catch
                        {
                            frame.Dispose();
                            throw;
                        }
                    }
                    else
                    {
                        // Without an allocator the session owns the bound:
                        // the policy is not in play (test/native paths).
                        if (length > maxFrameLength)
                            throw new ZeroMqProtocolException(
                                $"frame exceeds maximum length of {maxFrameLength} bytes");

                        decodedFrames.Add(StageDecoded(decoded));
                    }
                }
            }
            catch (Exception failure)
            {
                foreach (var frame in decodedFrames) frame.Dispose();

                _decodedFrames.Clear();
                unit.Message.Dispose();
                if (failure is ZeroMqProtocolException or ZReceiveRejectedException)
                    await CloseFaultAsync(failure);
                throw;
            }

            unit.Message.Dispose();
            return BuildMessage(decodedFrames);
        }
    }

    public void Dispose()
    {
        try
        {
            codec?.Dispose();
        }
        finally
        {
            if (_sendBuffer.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(_sendBuffer);
                _sendBuffer = [];
            }

            ReleaseStaging(_readStaging);
            _decodedFrames.Clear();
            _sendGate.Dispose();
        }
    }

    private ZMessage EncodeMessage(IZFrameCodec encoder, ZMessage message)
    {
        var frames = new ZFrame[message.Count];
        try
        {
            for (var i = 0; i < message.Count; i++)
            {
                var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                var encoded = encoder.Encode(new ZmtpFrameData
                {
                    Flags = flags,
                    Body = _logical.FromFrame(message[i]),
                });
                ZWireFrameLogic.Validate(encoded.Flags);
                frames[i] = StageAssembly(encoded);
            }

            return BuildMessage(frames);
        }
        catch
        {
            foreach (var frame in frames) frame.Dispose();

            throw;
        }
    }

    /// <summary>
    /// Stages one encoded frame over its OWN owner-backed buffer: every
    /// frame of the assembled message must stay alive until the message
    /// itself is disposed (by the sender on a copying connection, by
    /// the receiving chain on a retaining one).
    /// </summary>
    private ZFrame StageAssembly(ZmtpFrameData encoded)
    {
        var length = checked((int)encoded.Body.Length);
        var owner = pool.Rent(length == 0 ? 1 : length);
        try
        {
            encoded.Body.CopyTo(owner.Memory.Span);
            var more = (encoded.Flags & ZmtpFrameFlags.More) != 0;
            return new ZFrame(new ZSegment(owner, 0, length), more);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    private static ZMessage BuildMessage(List<ZFrame> frames)
        => frames.Count == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage([.. frames]));

    private static ZMessage BuildMessage(ZFrame[] frames)
        => frames.Length == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage(frames));

    /// <summary>Stages an encoded body over the reused send buffer (written before reuse).</summary>
    private ZFrame StageEncoded(ZmtpFrameData encoded)
    {
        var length = checked((int)encoded.Body.Length);
        if (_sendBuffer.Length < length)
        {
            if (_sendBuffer.Length > 0) ArrayPool<byte>.Shared.Return(_sendBuffer);
            _sendBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 4096));
        }

        encoded.Body.CopyTo(_sendBuffer.AsSpan(0, length));
        var more = (encoded.Flags & ZmtpFrameFlags.More) != 0;
        return new ZFrame(new ZSegment(_sendBuffer, 0, length), more);
    }

    private ZFrame StageDecoded(ZmtpFrameData decoded)
    {
        var length = checked((int)decoded.Body.Length);
        var buffer = RentStaging(_readStaging, length);
        decoded.Body.CopyTo(buffer.AsSpan(0, length));
        var more = (decoded.Flags & ZmtpFrameFlags.More) != 0;
        return new ZFrame(new ZSegment(buffer, 0, length), more);
    }

    private byte[] RentStaging(List<byte[]> staging, int length)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 4096));
        staging.Add(buffer);
        return buffer;
    }

    private static void ReleaseStaging(List<byte[]> staging)
    {
        foreach (var buffer in staging) ArrayPool<byte>.Shared.Return(buffer);

        staging.Clear();
    }

    /// <summary>The decoded body of a single-frame (command) message, staged in read buffers.</summary>
    private ReadOnlyMemory<byte> DecodeBody(IZFrameCodec decoder, ZMessage message)
    {
        var decoded = decoder.Decode(new ZmtpFrameData
        {
            Flags = ZmtpFrameFlags.Command,
            Body = message[0].ToSequence(),
        });
        ZWireFrameLogic.Validate(decoded.Flags);
        var length = checked((int)decoded.Body.Length);
        var buffer = RentStaging(_readStaging, length);
        decoded.Body.CopyTo(buffer.AsSpan(0, length));
        return buffer.AsMemory(0, length);
    }

    private static void CheckCommand(ReadOnlySpan<byte> body)
    {
        if (!ZmtpCommandCodec.TryReadCommandName(body, out var name))
            throw new ZeroMqProtocolException("malformed command name");
        if (name.SequenceEqual("ERROR"u8))
            throw new ZeroMqProtocolException(
                $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(body[(1 + name.Length)..])}");
    }

    private async ValueTask FailAsync(Exception failure)
    {
        if (failure is ZeroMqProtocolException or ZReceiveRejectedException)
            await CloseFaultAsync(failure);

        if (retire is not null) retire(failure);
        else connection.Abort();
    }

    private async ValueTask CloseFaultAsync(Exception failure)
    {
        if (connection is IZCloseHandshake close)
            await close.CloseAsync(ClassifyClose(failure), CancellationToken.None);
    }

    private static ZCloseReason ClassifyClose(Exception failure)
        => failure is ZReceiveRejectedException
        {
            Rejection.Reason: ZReceiveRejectionReason.FrameTooLarge
                or ZReceiveRejectionReason.MessageTooLarge,
        }
            ? ZCloseReason.MessageTooBig
            : ZCloseReason.ProtocolError;
}
