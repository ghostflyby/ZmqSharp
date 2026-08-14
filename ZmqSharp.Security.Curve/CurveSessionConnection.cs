using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve;

/// <summary>
/// Frame-level encrypt-on-write / decrypt-on-read session connection returned
/// by <see cref="CurveMechanism"/>. Post-handshake traffic is carried as ZMTP
/// MESSAGE command frames: the wire frame is "\x07MESSAGE" + 8-byte nonce tail
/// + box([flags][payload]), where the boxed flags are the original frame's
/// more/command bits. Decryption reassembles the plain frame (flags + size +
/// body) so <see cref="ZmtpParser"/> sees a normal frame stream; encryption
/// seals each frame the socket layer sends.
///
/// The hot path reuses per-connection buffers (0023): the seal buffer, the
/// read scratch, and the staged plain frame are connection fields, and the
/// nonce is a stack buffer, so steady-state frames allocate nothing. The
/// seal buffer is shared by all sends, so sealing plus the raw write run
/// under the send gate - the same serialization the clear path's
/// per-connection write gate provides (0021).
/// </summary>
public sealed class CurveSessionConnection : IZConnection
{
    private const int InitialBufferSize = 512;

    private readonly IZConnection raw;
    private readonly ICurveCryptoBackend crypto;
    private readonly Key32 boxKey;
    private readonly byte[] encodePrefix; // 16-byte nonce prefix for outbound frames
    private readonly byte[] decodePrefix; // 16-byte nonce prefix for inbound frames
    private ulong encodeNonce;
    private ulong decodeNonce;

    // Reused wire-frame buffer: SealFrame builds the frame here and hands it
    // to the raw connection; it stays valid until the write completes, so all
    // sealing + writing runs under the send gate.
    private byte[] sealBuffer = new byte[InitialBufferSize];
    private readonly SemaphoreSlim sendGate = new(1, 1);

    // Receive staging: scratch for wire reads plus the reconstructed plain
    // frame waiting for the parser.
    private byte[] readScratch = new byte[InitialBufferSize];
    private byte[]? pending;
    private int pendingLength;
    private int pendingOffset;

    // Send accumulation: WriteAsync receives arbitrary byte chunks (the
    // encoder writes header and body separately), so complete frames are
    // parsed out of the stream and sealed one at a time.
    private readonly List<byte> writeBuffer = [];
    private readonly Lock writeLock = new();

    internal CurveSessionConnection(
        IZConnection raw,
        ICurveCryptoBackend crypto,
        Key32 boxKey,
        byte[] encodePrefix,
        byte[] decodePrefix,
        ulong initialEncodeNonce,
        ulong initialDecodeNonce)
    {
        this.raw = raw;
        this.crypto = crypto;
        this.boxKey = boxKey;
        this.encodePrefix = encodePrefix;
        this.decodePrefix = decodePrefix;
        encodeNonce = initialEncodeNonce;
        decodeNonce = initialDecodeNonce;
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        // Serve from the staged plaintext frame first. The staging array stays
        // allocated across frames (0023); "has data" is pendingOffset <
        // pendingLength, never nulling the buffer, so the next frame reuses it.
        if (pending is { } staged && pendingOffset < pendingLength)
        {
            var available = pendingLength - pendingOffset;
            var count = Math.Min(buffer.Length, available);
            staged.AsSpan(pendingOffset, count).CopyTo(buffer.Span);
            pendingOffset += count;
            return ValueTask.FromResult(count);
        }

        return ReadFrameAsync(buffer, token);
    }

    private async ValueTask<int> ReadFrameAsync(Memory<byte> buffer, CancellationToken token)
    {
        // Read one wire frame: clear header, MESSAGE body, then decrypt.
        // libzmq carries CURVE traffic as plain data frames (no Command bit)
        // whose body begins with the "MESSAGE" literal; read the frame
        // regardless of flags and key on the literal.
        if (!await TryReadExactlyAsync(readScratch.AsMemory(0, 1), token)) return 0;

        var flags = readScratch[0];
        if ((flags & 0b1111_1000) != 0)
            throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

        var isLong = (flags & 0b0010) != 0;
        var headerLength = isLong ? 9 : 2;
        if (!await TryReadExactlyAsync(readScratch.AsMemory(1, isLong ? 8 : 1), token)) return 0;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(readScratch.AsSpan(1, 8))
            : readScratch[1];
        if (size < 0 || size > int.MaxValue) throw new ZeroMqProtocolException("invalid CURVE frame size");

        var bodyLength = (int)size;
        EnsureScratchCapacity(headerLength + bodyLength);
        if (!await TryReadExactlyAsync(readScratch.AsMemory(headerLength, bodyLength), token)) return 0;

        var body = readScratch.AsSpan(headerLength, bodyLength);
        if (bodyLength < 32 || !body[..8].SequenceEqual(CurveConstants.MessageLiteral))
            throw new ZeroMqProtocolException("CURVE traffic frame is missing the MESSAGE literal");

        var nonceTail = BinaryPrimitives.ReadUInt64BigEndian(body[8..16]);
        if (nonceTail <= decodeNonce)
            throw new ZeroMqProtocolException("CURVE frame nonce is not increasing (replay?)");
        decodeNonce = nonceTail;

        Span<byte> nonce = stackalloc byte[24];
        decodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], nonceTail);

        // Reconstruct the plain frame: the boxed plaintext is
        // [logical-flags][payload] (the logical more/command bits, distinct
        // from the wire frame's clear flags), and the parser expects
        // [flags][size][payload]. The box opens into the staging buffer, then
        // the payload shifts right to make room for the size header, and the
        // LongSize bit is recombined when the payload needs the 9-byte form.
        // The wire body is [MESSAGE literal][nonce tail][tag][ciphertext], so
        // the decrypted plaintext is 32 bytes shorter than the body.
        var plainLength = bodyLength - 32;
        if (plainLength < 1) // an empty boxed frame (not even a flags byte) is malformed
            throw new ZeroMqProtocolException("CURVE frame body is empty");
        var payloadLength = plainLength - 1;
        var isLongFrame = payloadLength > 255;
        var frameHeaderLength = isLongFrame ? 9 : 2;
        EnsurePendingCapacity(frameHeaderLength + payloadLength);
        if (pending is not { } staged)
            throw new InvalidOperationException("staging buffer was not allocated");
        if (!crypto.TrySecretBoxOpen(body[16..], nonce, boxKey.Span, staged.AsSpan(0, plainLength),
                out var written)
            || written != plainLength)
            throw new ZeroMqProtocolException("CURVE frame authentication failed");

        Array.Copy(staged, 1, staged, frameHeaderLength, payloadLength); // overlap-safe shift
        if (isLongFrame)
        {
            staged[0] |= 0b0010; // ZmtpFrameFlags.LongSize, combined with the boxed logical flags
            BinaryPrimitives.WriteInt64BigEndian(staged.AsSpan(1), payloadLength);
        }
        else staged[1] = (byte)payloadLength;
        pendingLength = frameHeaderLength + payloadLength;
        pendingOffset = 0;
        return await ReadAsync(buffer, token);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        // The socket encoder writes header and body separately; accumulate and
        // seal complete frames.
        lock (writeLock)
        {
            writeBuffer.AddRange(bytes.Span);
        }

        while (true)
        {
            byte[] frame;
            lock (writeLock)
            {
                frame = TakeCompleteFrame() ?? [];
            }

            if (frame.Length == 0) return;

            await SendSealedAsync(frame, ZmtpFrameFlags.None, token);
        }
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
    {
        return SendSealedAsync(frame, more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None, token);
    }

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return SendSealedAsync(body, ZmtpFrameFlags.Command, token);
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        // The whole message is sealed and written under one gate hold: the
        // seal buffer is shared, and message-level atomicity must survive
        // concurrent sends (0023, 0021 - the clear path serializes whole
        // messages under the connection write gate).
        await sendGate.WaitAsync(token);
        try
        {
            for (var i = 0; i < message.Count; i++)
            {
                var more = i < message.Count - 1;
                var flags = more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                var frame = message[i];
                if (frame.TryGetValue(out ZSegment single))
                {
                    await SendFrameLockedAsync(single.Memory, flags, token);
                }
                else
                {
                    // A multi-segment frame is one logical frame; its content
                    // is concatenated before sealing (the wire frame carries a
                    // single size). The rental lives only for the synchronous
                    // seal, so it is returned right after the write completes.
                    var sequence = frame.ToSequence();
                    if (sequence.IsSingleSegment)
                    {
                        await SendFrameLockedAsync(sequence.First, flags, token);
                    }
                    else
                    {
                        var rented = ArrayPool<byte>.Shared.Rent((int)sequence.Length);
                        try
                        {
                            sequence.CopyTo(rented);
                            await SendFrameLockedAsync(rented.AsMemory(0, (int)sequence.Length), flags, token);
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(rented);
                        }
                    }
                }
            }
        }
        finally
        {
            sendGate.Release();
        }
    }

    public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
    {
        raw.SetFrameHandler(onFrame);
    }

    public void SetConnectionEndedHandler(Action onConnectionEnded)
    {
        raw.SetConnectionEndedHandler(onConnectionEnded);
    }

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        return raw.OnFrameAsync(frame, token);
    }

    public void OnConnectionEnded()
    {
        raw.OnConnectionEnded();
    }

    public void Dispose()
    {
        // The send gate is deliberately not disposed here: an in-flight send
        // releases it in its finally, and disposing it mid-send would fault
        // that release with ObjectDisposedException. The raw connection owns
        // the socket teardown; the gate is only a buffer-ownership guard.
        raw.Dispose();
    }

    // ---- Frame sealing and wire reading ----

    /// <summary>
    /// Seals one logical frame into a MESSAGE wire frame, written into the
    /// reused seal buffer, and returns a view of it. The buffer is shared, so
    /// callers must run under the send gate until the raw write completes.
    /// </summary>
    private ReadOnlyMemory<byte> SealFrame(ReadOnlySpan<byte> payload, ZmtpFrameFlags flags)
    {
        var plainLength = 1 + payload.Length;
        var outputLength = 16 + plainLength; // tag + plaintext
        var bodyLength = 8 + 8 + outputLength; // MESSAGE literal + nonce tail + box
        var isLong = bodyLength > 255;
        var headerLength = isLong ? 9 : 2;
        var wireLength = headerLength + bodyLength;
        EnsureSealCapacity(wireLength);

        var wire = sealBuffer.AsSpan(0, wireLength);
        // libzmq carries CURVE traffic as plain data frames (no Command bit);
        // the "MESSAGE" literal inside the body is what identifies it.
        wire[0] = (byte)(isLong ? 0b0010 : 0);
        if (isLong) BinaryPrimitives.WriteInt64BigEndian(wire[1..9], bodyLength);
        else wire[1] = (byte)bodyLength;

        CurveConstants.MessageLiteral.CopyTo(wire[headerLength..]);
        BinaryPrimitives.WriteUInt64BigEndian(wire[(headerLength + 8)..], encodeNonce);
        var nonceTail = encodeNonce;
        encodeNonce++;

        // Box in place: the plaintext goes into the ciphertext region, the
        // backend writes tag(16) + ciphertext over the whole box region. The
        // boxed flags byte is the reconstructed frame's flag byte after
        // decryption, so it uses the real ZMTP values (More=0x01, Command=0x04)
        // - never 0x02, which would collide with LongSize on reconstruction.
        var box = wire[(headerLength + 16)..];
        box[16] = (byte)((flags & ZmtpFrameFlags.More) != 0
            ? 0x01
            : (flags & ZmtpFrameFlags.Command) != 0
                ? 0x04
                : 0x00);
        payload.CopyTo(box[17..]);

        Span<byte> nonce = stackalloc byte[24];
        encodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], nonceTail);
        crypto.SecretBox(box[16..], nonce, boxKey.Span, box);

        return sealBuffer.AsMemory(0, wireLength);
    }

    /// <summary>
    /// Seals <paramref name="payload"/> into the shared buffer and writes it
    /// under the send gate, which keeps the buffer valid for the whole write.
    /// </summary>
    private async ValueTask SendSealedAsync(ReadOnlyMemory<byte> payload, ZmtpFrameFlags flags,
        CancellationToken token)
    {
        await sendGate.WaitAsync(token);
        try
        {
            await SendFrameLockedAsync(payload, flags, token);
        }
        finally
        {
            sendGate.Release();
        }
    }

    /// <summary>Seals and writes one frame; the caller must hold the send gate.</summary>
    private async ValueTask SendFrameLockedAsync(ReadOnlyMemory<byte> payload, ZmtpFrameFlags flags,
        CancellationToken token)
    {
        var wire = SealFrame(payload.Span, flags);
        await raw.WriteAsync(wire, token);
    }

    /// <summary>Pulls one complete ZMTP frame out of the write buffer, or null if the buffer holds a partial frame.</summary>
    private byte[]? TakeCompleteFrame()
    {
        var buffer = writeBuffer;
        if (buffer.Count == 0) return null;

        var flags = buffer[0];
        if ((flags & 0b1111_1000) != 0) throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

        var isLong = (flags & 0b0010) != 0;
        var headerLength = isLong ? 9 : 2;
        if (buffer.Count < headerLength) return null;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(CollectionsMarshal.AsSpan(buffer).Slice(1, 8))
            : buffer[1];
        if (size < 0 || size > int.MaxValue) throw new ZeroMqProtocolException("invalid ZMTP frame size");

        var total = headerLength + (int)size;
        if (buffer.Count < total) return null;

        var frame = buffer.GetRange(0, total).ToArray();
        buffer.RemoveRange(0, total);
        return frame;
    }

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await raw.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }

    private void EnsureScratchCapacity(int required)
    {
        if (readScratch.Length >= required) return;

        var newSize = Math.Max(required, readScratch.Length * 2);
        Array.Resize(ref readScratch, newSize);
    }

    private void EnsurePendingCapacity(int required)
    {
        if (pending is not null && pending.Length >= required) return;

        var newSize = Math.Max(required, (pending?.Length ?? InitialBufferSize) * 2);
        pending = new byte[newSize];
    }

    private void EnsureSealCapacity(int required)
    {
        if (sealBuffer.Length >= required) return;

        var newSize = Math.Max(required, sealBuffer.Length * 2);
        Array.Resize(ref sealBuffer, newSize);
    }
}
