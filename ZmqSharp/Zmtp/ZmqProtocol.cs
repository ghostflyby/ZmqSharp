using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The built-in ZMTP 3.0 protocol over the frame layer (0033): the 64-byte
/// greeting exchange (raw on byte-origin connections - wire-identical to
/// every ZMTP peer - and a command frame on native frame connections), the
/// configured security mechanism's command exchange, and frame-per-message
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
        IZFrameConnection frames, IZConnection? byteConnection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        MemoryPool<byte>? pool = null)
        => new Establisher(frames, byteConnection, mechanism, localReadyBody, maxCommandSize);

    public IZWireSession CreateSession(IZFrameConnection frames, IZFrameCodec? codec, Action<Exception>? retire = null)
        => new Session(frames, codec, retire);

    internal static void CheckCommand(ReadOnlySpan<byte> body)
    {
        if (!ZmtpCommandCodec.TryReadCommandName(body, out var name))
            throw new ZeroMqProtocolException("malformed command name");
        if (name.SequenceEqual("ERROR"u8))
            throw new ZeroMqProtocolException(
                $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(body[(1 + name.Length)..])}");
    }

    private sealed class Establisher(
        IZFrameConnection frames,
        IZConnection? byteConnection,
        IZSecurityMechanism mechanism,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize) : IZWireEstablisher
    {
        public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
        {
            // The 64-byte greeting is a stream construct: raw on the byte
            // origin (wire-identical to every ZMTP peer), a command frame on
            // native frame connections.
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
                await WritePooledAsync(frames, ZmtpFrameFlags.Command,
                    ZmtpGreeting.Build(mechanism.Name, mechanism.Role), token);

                if (await frames.ReadFrameAsync(token) is not { } reply) return null;

                using (reply.Frame)
                {
                    if ((reply.Flags & ZmtpFrameFlags.Command) != 0 && reply.BodyLength == 64)
                        peerMechanism = ZmtpGreeting.ParseMechanism(ZWireFrame.Contiguous(reply.Frame).Span);
                    else
                        throw new ZeroMqProtocolException("expected the greeting as the first command frame");
                }
            }

            if (!string.Equals(peerMechanism, mechanism.Name, StringComparison.Ordinal))
            {
                await WritePooledAsync(frames, ZmtpFrameFlags.Command,
                    ZmtpCommands.BuildError("Invalid mechanism"), token);
                throw new ZeroMqProtocolException(
                    $"peer mechanism '{peerMechanism}' does not match the configured mechanism '{mechanism.Name}'");
            }

            using var context = new ZMechanismContext(
                new FrameCommandChannel(frames, maxCommandSize), localReadyBody, maxCommandSize);
            return await mechanism.CreateSession().RunAsync(context, token);
        }

        private static async ValueTask WritePooledAsync(
            IZFrameConnection frames, ZmtpFrameFlags flags, byte[] body, CancellationToken token)
        {
            // The pooled copy travels WITH the frame: a native frame transport
            // carries it to the peer, whose reader disposes it; a framing
            // adapter copies it out to the wire. Either way the sender must
            // NOT release the owner at write completion (0033).
            await frames.WriteFrameAsync(ZWireFrame.Copy(flags, body).Wire, token);
        }

        public void Dispose()
        {
            // The mechanism context and its channel were disposed when
            // EstablishAsync returned; nothing remains to release.
        }
    }

    private sealed class Session(
        IZFrameConnection frames, IZFrameCodec? codec, Action<Exception>? retire) : IZWireSession
    {
        private readonly SemaphoreSlim _sendGate = new(1, 1);
        private readonly FrameSequence _logical = new();

        // Codec output staging reused across sends: the send gate is the
        // single writer, so one growing buffer serves every encoded frame
        // without a per-frame pool rent (an owner allocation, 0033).
        private IMemoryOwner<byte>? _encodeOwner;
        private Memory<byte> _encodeBuffer;

        public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            await _sendGate.WaitAsync(token);
            try
            {
                for (var i = 0; i < message.Count; i++)
                {
                    var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                    await SendAsync(flags, _logical.FromFrame(message[i]), borrowedFrame: message[i], token);
                }
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
                await SendAsync(ZmtpFrameFlags.Command, new ReadOnlySequence<byte>(body), borrowedFrame: null, token);
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

        public IZWirePump CreatePump(
            Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
            ZFrameAllocator? allocator, long maxCommandSize, long maxFrameLength)
            => new Pump(frames, codec, onFrame, allocator, maxCommandSize, maxFrameLength);

        public void Dispose()
        {
            try
            {
                codec?.Dispose();
            }
            finally
            {
                try
                {
                    _encodeOwner?.Dispose();
                    _encodeOwner = null;
                    _encodeBuffer = Memory<byte>.Empty;
                }
                finally
                {
                    _sendGate.Dispose();
                }
            }
        }

        /// <summary>
        /// Sends one logical frame. A message's own frame is borrowed (the
        /// runtime disposes the message after the send); command bodies and
        /// codec-encoded bodies are copied into pooled frames that this send
        /// releases once the write completes.
        /// </summary>
        private async ValueTask SendAsync(
            ZmtpFrameFlags flags, ReadOnlySequence<byte> logicalBody, ZFrame? borrowedFrame, CancellationToken token)
        {
            ZWireFrame wire;
            if (codec is { } transform)
            {
                var encoded = transform.Encode(new ZmtpFrameData { Flags = flags, Body = logicalBody });
                ZWireFrame.Validate(encoded.Flags);
                wire = CopyIntoStaging(encoded.Flags, encoded.Body);
            }
            else if (borrowedFrame is { } frame)
            {
                wire = new ZWireFrame(flags, frame);
            }
            else
            {
                wire = CopyIntoStaging(flags, logicalBody);
            }

            await frames.WriteFrameAsync(wire, token);
        }

        /// <summary>Copies a body into the reused staging buffer as a frame.</summary>
        private ZWireFrame CopyIntoStaging(ZmtpFrameFlags flags, ReadOnlySequence<byte> body)
        {
            var length = checked((int)body.Length);
            if (_encodeBuffer.Length < length)
            {
                var owner = MemoryPool<byte>.Shared.Rent(Math.Max(length, Math.Max(4096, _encodeBuffer.Length * 2)));
                _encodeOwner?.Dispose();
                _encodeOwner = owner;
                _encodeBuffer = owner.Memory;
            }

            body.CopyTo(_encodeBuffer.Span);
            var more = (flags & ZmtpFrameFlags.More) != 0;
            // A borrowed view of the staging buffer: an accidental dispose of
            // the wire frame cannot release the reused owner.
            return new ZWireFrame(flags, new ZFrame(ZSegment.Borrowed(_encodeBuffer[..length]), more));
        }

        private void Fail(Exception failure)
        {
            if (retire is not null) retire(failure);
            else frames.Abort();
        }
    }

    private sealed class Pump(
        IZFrameConnection frames,
        IZFrameCodec? codec,
        Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
        ZFrameAllocator? allocator,
        long maxCommandSize,
        long maxFrameLength) : IZWirePump
    {
        private readonly long _maxEncodedLength =
            codec?.GetMaximumEncodedLength(Math.Max(maxFrameLength, maxCommandSize)) ?? long.MaxValue;

        private readonly Lock _gateLock = new();
        private TaskCompletionSource _gate = CreateGate();

        public async ValueTask ParseAsync(CancellationToken token = default)
        {
            while (true)
            {
                if (await frames.ReadFrameAsync(token) is not { } wire) return;

                if (codec is { } transform)
                {
                    // The encoded source frame is released as soon as the
                    // decode consumed it.
                    using (wire.Frame)
                    {
                        if (wire.BodyLength > _maxEncodedLength || wire.BodyLength > int.MaxValue)
                            throw new ZeroMqProtocolException("encoded frame exceeds maximum supported length");

                        var decoded = transform.Decode(new ZmtpFrameData
                        {
                            Flags = wire.Flags,
                            Body = wire.Frame.ToSequence(),
                        });
                        ZWireFrame.Validate(decoded.Flags);
                        if ((decoded.Flags & ZmtpFrameFlags.Command) != 0)
                        {
                            if (decoded.Body.Length > maxCommandSize)
                                throw new ZeroMqProtocolException(
                                    $"command frame exceeds maximum size of {maxCommandSize} bytes");
                            if (!decoded.Body.IsSingleSegment)
                                throw new ZeroMqProtocolException("decoded command must be contiguous");
                            CheckCommand(decoded.Body.First.Span);
                            continue;
                        }

                        var more = (decoded.Flags & ZmtpFrameFlags.More) != 0;
                        ZFrame delivered;
                        if (allocator is { } materialize)
                        {
                            // The receive policy's final allocation: the
                            // decoded frame lands directly in its segments
                            // (limits reject before any rent, 0008).
                            delivered = materialize(checked((int)decoded.Body.Length), more);
                            try
                            {
                                var remaining = decoded.Body;
                                foreach (var segment in delivered)
                                {
                                    remaining.Slice(0, segment.Memory.Length).CopyTo(segment.Writable.Span);
                                    remaining = remaining.Slice(segment.Memory.Length);
                                }
                            }
                            catch
                            {
                                delivered.Dispose();
                                throw;
                            }
                        }
                        else
                        {
                            // Without an allocator the pump owns the bound:
                            // the policy is not in play (test/native paths).
                            if (decoded.Body.Length > maxFrameLength)
                                throw new ZeroMqProtocolException(
                                    $"frame exceeds maximum length of {maxFrameLength} bytes");

                            if (decoded.Body.IsSingleSegment)
                            {
                                // Borrowed from the codec's buffer: valid until the
                                // next decode, which happens only after delivery.
                                delivered = new ZFrame(ZSegment.Borrowed(decoded.Body.First), more);
                            }
                            else
                            {
                                var length = checked((int)decoded.Body.Length);
                                var owner = MemoryPool<byte>.Shared.Rent(length);
                                try
                                {
                                    decoded.Body.CopyTo(owner.Memory.Span);
                                    delivered = new ZFrame(new ZSegment(owner, 0, length), more);
                                }
                                catch
                                {
                                    owner.Dispose();
                                    throw;
                                }
                            }
                        }

                        if (!await onFrame(this, delivered, token)) await WaitForResumeAsync(token);
                        continue;
                    }
                }

                if ((wire.Flags & ZmtpFrameFlags.Command) != 0)
                {
                    if (wire.BodyLength > maxCommandSize)
                        throw new ZeroMqProtocolException(
                            $"command frame exceeds maximum size of {maxCommandSize} bytes");
                    CheckCommand(ZWireFrame.Contiguous(wire.Frame).Span);
                    wire.Frame.Dispose();
                    continue;
                }

                // The delivered frame must carry the WIRE more flag: a
                // borrowed message frame's own More is not authoritative (the
                // session computes flags per message position), so rewrap the
                // same segments when they disagree - a struct copy, the
                // handler's disposal covers the owner once either way.
                var wireMore = (wire.Flags & ZmtpFrameFlags.More) != 0;
                var logical = wireMore == wire.Frame.More
                    ? wire.Frame
                    : wire.Frame.TryGetValue(out ZSegments many)
                        ? new ZFrame(many, wireMore)
                        : new ZFrame(wire.Frame.TryGetValue(out ZSegment single) ? single : default, wireMore);

                // The handler receives an owned frame and owns its disposal
                // or transfer (0033): pooled frames flow to queues, the raw
                // callback tier disposes after delivery.
                if (!await onFrame(this, logical, token)) await WaitForResumeAsync(token);
            }
        }

        public void Resume()
        {
            lock (_gateLock)
            {
                _gate.TrySetResult();
                _gate = CreateGate();
            }
        }

        private static TaskCompletionSource CreateGate()
        {
            return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private async ValueTask WaitForResumeAsync(CancellationToken token)
        {
            Task task;
            lock (_gateLock)
            {
                task = _gate.Task;
            }

            await task.WaitAsync(token);
        }
    }
}
