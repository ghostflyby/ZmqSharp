using System.Buffers.Binary;
using System.Runtime.InteropServices;
using ZmqSharp.Messages;
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
/// </summary>
public sealed class CurveSessionConnection : IZConnection
{
    private const byte CommandFlag = 0b0100;

    private readonly IZConnection raw;
    private readonly ICurveCryptoBackend crypto;
    private readonly byte[] boxKey;
    private readonly byte[] encodePrefix;
    private readonly byte[] decodePrefix;
    private ulong encodeNonce;
    private ulong decodeNonce;

    // Receive staging: the decrypted frame bytes waiting for the parser.
    private byte[]? pending;
    private int pendingOffset;

    // Send accumulation: WriteAsync receives arbitrary byte chunks (the
    // encoder writes header and body separately), so complete frames are
    // parsed out of the stream and sealed one at a time.
    private readonly List<byte> writeBuffer = [];
    private readonly Lock writeLock = new();

    internal CurveSessionConnection(
        IZConnection raw,
        ICurveCryptoBackend crypto,
        byte[] boxKey,
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
        // Serve from the staged plaintext frame first.
        if (pending is not null)
        {
            var available = pending.Length - pendingOffset;
            var count = Math.Min(buffer.Length, available);
            pending.AsSpan(pendingOffset, count).CopyTo(buffer.Span);
            pendingOffset += count;
            if (pendingOffset == pending.Length)
            {
                pending = null;
                pendingOffset = 0;
            }

            return ValueTask.FromResult(count);
        }

        return ReadFrameAsync(buffer, token);
    }

    private async ValueTask<int> ReadFrameAsync(Memory<byte> buffer, CancellationToken token)
    {
        // Read one wire frame: clear header, MESSAGE body, then decrypt.
        var flags = await ReadExactlyAsync(1, token);
        if (flags is null) return 0;

        if ((flags[0] & CommandFlag) == 0)
            throw new ZeroMqProtocolException("CURVE traffic frame is not a MESSAGE command frame");
        if ((flags[0] & 0b0001) != 0)
            throw new ZeroMqProtocolException("CURVE traffic frame carries the MORE flag");

        var isLong = (flags[0] & 0b0010) != 0;
        var sizeBytes = await ReadExactlyAsync(isLong ? 8 : 1, token);
        if (sizeBytes is null) return 0;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(sizeBytes)
            : sizeBytes[0];
        if (size < 0 || size > int.MaxValue) throw new ZeroMqProtocolException("invalid CURVE frame size");

        var body = await ReadExactlyAsync((int)size, token);
        if (body is null) return 0;

        if (body.Length < 32 || !body.AsSpan(0, 8).SequenceEqual(CurveConstants.MessageLiteral))
            throw new ZeroMqProtocolException("CURVE traffic frame is missing the MESSAGE literal");

        var nonceTail = BinaryPrimitives.ReadUInt64BigEndian(body.AsSpan(8, 8));
        if (nonceTail <= decodeNonce)
            throw new ZeroMqProtocolException("CURVE frame nonce is not increasing (replay?)");
        decodeNonce = nonceTail;

        var nonce = new byte[24];
        decodePrefix.CopyTo(nonce, 0);
        BinaryPrimitives.WriteUInt64BigEndian(nonce.AsSpan(16), nonceTail);

        var plaintext = crypto.SecretBoxOpen(body.AsSpan(16), nonce, boxKey)
                        ?? throw new ZeroMqProtocolException("CURVE frame authentication failed");

        // Reconstruct the plain frame: [flags][size][payload].
        var payload = plaintext.AsSpan(1);
        var headerLength = payload.Length > 255 ? 9 : 2;
        var frame = new byte[headerLength + payload.Length];
        frame[0] = plaintext[0];
        if (headerLength == 9) BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(1), payload.Length);
        else frame[1] = (byte)payload.Length;
        payload.CopyTo(frame.AsSpan(headerLength));

        pending = frame;
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

            await raw.WriteAsync(SealFrame(frame, flags: 0), token);
        }
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
    {
        return raw.WriteAsync(
            SealFrame(frame.Span, more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None), token);
    }

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return raw.WriteAsync(SealFrame(body.Span, ZmtpFrameFlags.Command), token);
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        for (var i = 0; i < message.Count; i++)
        {
            var more = i < message.Count - 1;
            var frame = message[i].ToSequence();
            var flags = more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
            await raw.WriteAsync(SealFrame(frame.FirstSpan, flags), token);
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
        raw.Dispose();
    }

    // ---- Frame sealing and wire reading ----

    /// <summary>Seals one logical frame into a MESSAGE wire frame (header + body).</summary>
    private byte[] SealFrame(ReadOnlySpan<byte> payload, ZmtpFrameFlags flags)
    {
        var plaintext = new byte[1 + payload.Length];
        plaintext[0] = (byte)((flags & ZmtpFrameFlags.More) != 0
                              ? 0x01
                              : ((flags & ZmtpFrameFlags.Command) != 0 ? 0x02 : 0x00));
        payload.CopyTo(plaintext.AsSpan(1));

        var nonce = new byte[24];
        encodePrefix.CopyTo(nonce, 0);
        BinaryPrimitives.WriteUInt64BigEndian(nonce.AsSpan(16), encodeNonce);
        var nonceTail = encodeNonce;
        encodeNonce++;

        var box = crypto.SecretBox(plaintext, nonce, boxKey);

        var body = new byte[8 + 8 + box.Length];
        CurveConstants.MessageLiteral.CopyTo(body, 0);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8), nonceTail);
        box.CopyTo(body, 16);

        var isLong = body.Length > 255;
        var wire = new byte[(isLong ? 9 : 2) + body.Length];
        wire[0] = (byte)(CommandFlag | (isLong ? 0b0010 : 0));
        if (isLong) BinaryPrimitives.WriteInt64BigEndian(wire.AsSpan(1), body.Length);
        else wire[1] = (byte)body.Length;
        body.CopyTo(wire.AsSpan(isLong ? 9 : 2));
        return wire;
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

        long size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(CollectionsMarshal.AsSpan(buffer).Slice(1, 8))
            : buffer[1];
        if (size < 0 || size > int.MaxValue) throw new ZeroMqProtocolException("invalid ZMTP frame size");

        var total = headerLength + (int)size;
        if (buffer.Count < total) return null;

        var frame = buffer.GetRange(0, total).ToArray();
        buffer.RemoveRange(0, total);
        return frame;
    }

    private async ValueTask<byte[]?> ReadExactlyAsync(int count, CancellationToken token)
    {
        var buffer = new byte[count];
        var filled = 0;
        while (filled < count)
        {
            var read = await raw.ReadAsync(buffer.AsMemory(filled), token);
            if (read == 0) return null;

            filled += read;
        }

        return buffer;
    }
}
