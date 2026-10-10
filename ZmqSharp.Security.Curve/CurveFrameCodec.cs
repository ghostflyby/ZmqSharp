using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve;

/// <summary>Authenticated CURVE frame transformation; never reconstructs a plaintext wire header.</summary>
public sealed class CurveFrameCodec : IZFrameCodec
{
    private readonly ICurveCryptoBackend crypto;
    private Key32 key;
    private readonly bool encodeServerToClient;
    private readonly bool decodeServerToClient;
    private byte[]? seal;
    private byte[]? plain;
    private byte[]? gathered;
    private ulong encodeNonce;
    private ulong decodeNonce;
    private bool disposed;

    internal CurveFrameCodec(ICurveCryptoBackend crypto, Key32 key,
        bool encodeServerToClient, bool decodeServerToClient,
        ulong encodeNonce, ulong decodeNonce)
    {
        this.crypto = crypto;
        this.key = key;
        this.encodeServerToClient = encodeServerToClient;
        this.decodeServerToClient = decodeServerToClient;
        this.encodeNonce = encodeNonce;
        this.decodeNonce = decodeNonce;
    }

    public long GetMaximumEncodedLength(long logicalBodyLimit)
        => logicalBodyLimit > long.MaxValue - 33 ? long.MaxValue : checked(logicalBodyLimit + 33);

    public ZmtpFrameData Encode(ZmtpFrameData logicalFrame)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if ((logicalFrame.Flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            logicalFrame.Flags == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid CURVE logical flags");
        if (encodeNonce == ulong.MaxValue) throw new ZeroMqProtocolException("CURVE nonce exhausted");
        var length = checked((int)logicalFrame.Body.Length + 33);
        var buffer = Ensure(ref seal, length);
        var body = buffer.AsSpan(0, length);
        CurveConstants.MessageLiteral.CopyTo(body);
        BinaryPrimitives.WriteUInt64BigEndian(body[8..16], encodeNonce);
        var box = body[16..];
        box[16] = (byte)logicalFrame.Flags;
        logicalFrame.Body.CopyTo(box[17..]);
        Span<byte> nonce = stackalloc byte[24];
        var encodePrefix = encodeServerToClient
            ? CurveConstants.MessagePrefixServerToClient
            : CurveConstants.MessagePrefixClientToServer;
        encodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], encodeNonce++);
        crypto.SecretBox(box[16..], nonce, key.Span, box);
        return new ZmtpFrameData { Body = new ReadOnlySequence<byte>(buffer.AsMemory(0, length)) };
    }

    public ZmtpFrameData Decode(ZmtpFrameData wireFrame)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ReadOnlyMemory<byte> memory;
        var length = checked((int)wireFrame.Body.Length);
        if (wireFrame.Body.IsSingleSegment) memory = wireFrame.Body.First;
        else
        {
            var buffer = Ensure(ref gathered, length);
            wireFrame.Body.CopyTo(buffer);
            memory = buffer.AsMemory(0, length);
        }

        var body = memory.Span;
        if (body.Length < 33 || !body[..8].SequenceEqual(CurveConstants.MessageLiteral))
            throw new ZeroMqProtocolException("CURVE traffic frame is missing the MESSAGE literal");
        var tail = BinaryPrimitives.ReadUInt64BigEndian(body[8..16]);
        if (tail <= decodeNonce) throw new ZeroMqProtocolException("CURVE frame nonce is not increasing (replay?)");
        Span<byte> nonce = stackalloc byte[24];
        var decodePrefix = decodeServerToClient
            ? CurveConstants.MessagePrefixServerToClient
            : CurveConstants.MessagePrefixClientToServer;
        decodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], tail);
        var plaintextLength = length - 32;
        var output = Ensure(ref plain, plaintextLength);
        if (!crypto.TrySecretBoxOpen(body[16..], nonce, key.Span, output.AsSpan(0, plaintextLength), out var written)
            || written != plaintextLength)
            throw new ZeroMqProtocolException("CURVE frame authentication failed");
        var flags = (ZmtpFrameFlags)output[0];
        if ((flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            flags == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid CURVE logical flags");
        decodeNonce = tail;
        return new ZmtpFrameData { Flags = flags, Body = new(output.AsMemory(1, plaintextLength - 1)) };
    }

    private static byte[] Ensure(ref byte[]? field, int length)
    {
        if (field is { } existing && existing.Length >= length) return existing;
        if (field is { } old) ArrayPool<byte>.Shared.Return(old, clearArray: true);
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(256, length));
        field = buffer;
        return buffer;
    }

    private static void Return(byte[]? buffer)
    {
        if (buffer is { } owned) ArrayPool<byte>.Shared.Return(owned, clearArray: true);
    }

    public void Dispose()
    {
        if (disposed) return;
        disposed = true;
        key = default;
        Return(seal);
        Return(plain);
        Return(gathered);
        seal = plain = gathered = null;
    }
}
