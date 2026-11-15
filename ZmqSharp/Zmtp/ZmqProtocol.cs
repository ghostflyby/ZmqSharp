using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The built-in ZMTP 3.0 protocol over the message layer (0033): the 64-byte
/// greeting exchange (raw on byte-origin connections - wire-identical to
/// every ZMTP peer - and one command on native message connections), the
/// configured security mechanism's command exchange, and message-per-unit
/// traffic through the optional codec. This is the protocol tcp/ipc have
/// always spoken, decomposed so its framing lives in
/// <see cref="ZmtpFraming"/> and its semantics here.
/// </summary>
public sealed class ZmqProtocol : IZWireProtocol
{
    public static ZmqProtocol Instance { get; } = new();

    private ZmqProtocol()
    {
    }

    public IZWireEstablisher CreateEstablisher(
        IZMessageConnection connection, IZConnection? byteConnection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null)
        => new Establisher(connection, byteConnection, mechanism, localReadyBody, maxCommandSize);

    public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue)
        => new Session(connection, codec, retire, decodedAllocator, maxCommandSize, maxFrameLength);

    internal static void CheckCommand(ReadOnlySpan<byte> body)
    {
        if (!ZmtpCommandCodec.TryReadCommandName(body, out var name))
            throw new ZeroMqProtocolException("malformed command name");
        if (name.SequenceEqual("ERROR"u8))
            throw new ZeroMqProtocolException(
                $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(body[(1 + name.Length)..])}");
    }

    private sealed class Establisher(
        IZMessageConnection connection,
        IZConnection? byteConnection,
        IZSecurityMechanism mechanism,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize) : IZWireEstablisher
    {
        public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
        {
            // The 64-byte greeting is a stream construct: raw on the byte
            // origin (wire-identical to every ZMTP peer), one command on
            // native message connections.
            string? peerMechanism;
            if (byteConnection is { } bytes)
            {
                await bytes.WriteAsync(ZmtpGreeting.Build(mechanism.Name, mechanism.Role), token);

                var greeting = new byte[64];
                var filled = 0;
                while (filled < greeting.Length)
                {
                    var count = await bytes.ReadAsync(greeting.AsMemory(filled), token);
                    if (count == 0) return null;

                    filled += count;
                }

                peerMechanism = ZmtpGreeting.ParseMechanism(greeting);
            }
            else
            {
                await connection.WriteCommandAsync(ZmtpGreeting.Build(mechanism.Name, mechanism.Role), token);

                if (await connection.ReadUnitAsync(token) is not { } reply) return null;

                using (reply.Message)
                {
                    if (!reply.IsCommand || reply.Message.Count != 1
                        || reply.Message[0].ToSequence().Length != 64)
                        throw new ZeroMqProtocolException("expected the greeting as the first command");

                    peerMechanism = ZmtpGreeting.ParseMechanism(
                        ZWireFrameLogic.Contiguous(reply.Message[0]).Span);
                }
            }

            if (!string.Equals(peerMechanism, mechanism.Name, StringComparison.Ordinal))
            {
                await connection.WriteCommandAsync(ZmtpCommands.BuildError("Invalid mechanism"), token);
                throw new ZeroMqProtocolException(
                    $"peer mechanism '{peerMechanism}' does not match the configured mechanism '{mechanism.Name}'");
            }

            using var context = new ZMechanismContext(
                new UnitCommandChannel(connection, maxCommandSize), localReadyBody, maxCommandSize);
            return await mechanism.CreateSession().RunAsync(context, token);
        }

        public void Dispose()
        {
            // The mechanism context and its channel were disposed when
            // EstablishAsync returned; nothing remains to release.
        }
    }

    private sealed class Session(
        IZMessageConnection connection, IZFrameCodec? codec, Action<Exception>? retire,
        ZFrameAllocator? decodedAllocator, long maxCommandSize, long maxFrameLength) : IZWireSession
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

        public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            await _sendGate.WaitAsync(token);
            try
            {
                if (codec is null)
                {
                    await connection.WriteMessageAsync(message, token);
                    return;
                }

                if (connection is ZmtpFraming.ZmtpFramingConnection streaming)
                {
                    // Built-in fast path: each frame is encoded into the
                    // reused send buffer and streamed as one logical write -
                    // the buffer is free again the moment the write completes
                    // (the gate serializes sends), so the steady state
                    // allocates nothing (0033).
                    for (var i = 0; i < message.Count; i++)
                    {
                        var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                        var encoded = codec.Encode(new ZmtpFrameData
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

                var assembled = EncodeMessage(message);
                await connection.WriteMessageAsync(assembled, token);
            }
            catch (Exception failure)
            {
                Fail(failure);
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

                var encoded = codec.Encode(new ZmtpFrameData
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
                Fail(failure);
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
                    using (unit.Message)
                    {
                        if (unit.Message.Count != 1)
                            throw new ZeroMqProtocolException("a command is a single-frame message");

                        if (unit.Message[0].ToSequence().Length > maxCommandSize)
                            throw new ZeroMqProtocolException(
                                $"command frame exceeds maximum size of {maxCommandSize} bytes");

                        var body = codec is null
                            ? ZWireFrameLogic.Contiguous(unit.Message[0])
                            : DecodeBody(unit.Message);
                        CheckCommand(body.Span);
                    }

                    continue;
                }

                if (codec is null) return unit.Message;

                // Codec sessions decode each frame of the aggregated
                // (encoded) message into fresh frames and rebuild; logical
                // limits apply after authentication (0008). Without a
                // decoded allocator the staged frames are valid until the
                // NEXT read on this session (documented window); the runtime
                // always supplies the materializer's allocator, which owns
                // its frames outright.
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
                        var decoded = codec.Decode(new ZmtpFrameData
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
                catch
                {
                    foreach (var frame in decodedFrames) frame.Dispose();

                    _decodedFrames.Clear();
                    unit.Message.Dispose();
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

        private ZMessage EncodeMessage(ZMessage message)
        {
            var frames = new ZFrame[message.Count];
            try
            {
                for (var i = 0; i < message.Count; i++)
                {
                    var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                    var encoded = codec!.Encode(new ZmtpFrameData
                    {
                        Flags = flags,
                        Body = _logical.FromFrame(message[i]),
                    });
                    ZWireFrameLogic.Validate(encoded.Flags);
                    frames[i] = StageEncoded(encoded);
                }

                return BuildMessage(frames);
            }
            catch
            {
                foreach (var frame in frames) frame.Dispose();

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
            if (_sendBuffer.Length < length) _sendBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 4096));
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
        private ReadOnlyMemory<byte> DecodeBody(ZMessage message)
        {
            var decoded = codec!.Decode(new ZmtpFrameData
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

        private void Fail(Exception failure)
        {
            if (retire is not null) retire(failure);
            else connection.Abort();
        }
    }
}
