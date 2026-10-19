using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve;

/// <summary>
/// Authenticated CURVE frame transformation; never reconstructs a plaintext
/// wire header. Created by <see cref="CurveMechanism"/> sessions and surfaced
/// to consumers as <see cref="IZFrameCodec"/> through
/// <see cref="ZMechanismResult.Codec"/>; the concrete type is an internal
/// detail.
/// </summary>
internal sealed class CurveFrameCodec : IZFrameCodec
{
    private Key32 _key;
    private readonly bool _encodeServerToClient;
    private readonly bool _decodeServerToClient;
    private byte[]? _seal;
    private byte[]? _plain;
    private byte[]? _gathered;
    private ulong _encodeNonce;
    private ulong _decodeNonce;
    private bool _disposed;

    internal CurveFrameCodec(Key32 key,
        bool encodeServerToClient, bool decodeServerToClient,
        ulong encodeNonce, ulong decodeNonce)
    {
        _key = key;
        _encodeServerToClient = encodeServerToClient;
        _decodeServerToClient = decodeServerToClient;
        _encodeNonce = encodeNonce;
        _decodeNonce = decodeNonce;
    }

    public long GetMaximumEncodedLength(long logicalBodyLimit)
        => logicalBodyLimit > long.MaxValue - 33 ? long.MaxValue : checked(logicalBodyLimit + 33);

    public ZmtpFrameData Encode(ZmtpFrameData logicalFrame)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((logicalFrame.Flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            logicalFrame.Flags == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid CURVE logical flags");
        if (_encodeNonce == ulong.MaxValue) throw new ZeroMqProtocolException("CURVE nonce exhausted");
        var length = checked((int)logicalFrame.Body.Length + 33);
        var buffer = Ensure(ref _seal, length);
        var body = buffer.AsSpan(0, length);
        CurveConstants.MessageLiteral.CopyTo(body);
        BinaryPrimitives.WriteUInt64BigEndian(body[8..16], _encodeNonce);
        var box = body[16..];
        box[16] = (byte)logicalFrame.Flags;
        logicalFrame.Body.CopyTo(box[17..]);
        Span<byte> nonce = stackalloc byte[24];
        var encodePrefix = _encodeServerToClient
            ? CurveConstants.MessagePrefixServerToClient
            : CurveConstants.MessagePrefixClientToServer;
        encodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], _encodeNonce++);
        CurveCrypto.SecretBox(box[16..], nonce, _key.Span, box);
        return new ZmtpFrameData { Body = new ReadOnlySequence<byte>(buffer.AsMemory(0, length)) };
    }

    public ZmtpFrameData Decode(ZmtpFrameData wireFrame)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ReadOnlyMemory<byte> memory;
        var length = checked((int)wireFrame.Body.Length);
        if (wireFrame.Body.IsSingleSegment) memory = wireFrame.Body.First;
        else
        {
            var buffer = Ensure(ref _gathered, length);
            wireFrame.Body.CopyTo(buffer);
            memory = buffer.AsMemory(0, length);
        }

        var body = memory.Span;
        if (body.Length < 33 || !body[..8].SequenceEqual(CurveConstants.MessageLiteral))
            throw new ZeroMqProtocolException("CURVE traffic frame is missing the MESSAGE literal");
        var tail = BinaryPrimitives.ReadUInt64BigEndian(body[8..16]);
        if (tail <= _decodeNonce) throw new ZeroMqProtocolException("CURVE frame nonce is not increasing (replay?)");
        Span<byte> nonce = stackalloc byte[24];
        var decodePrefix = _decodeServerToClient
            ? CurveConstants.MessagePrefixServerToClient
            : CurveConstants.MessagePrefixClientToServer;
        decodePrefix.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], tail);
        var plaintextLength = length - 32;
        var output = Ensure(ref _plain, plaintextLength);
        if (!CurveCrypto.TrySecretBoxOpen(body[16..], nonce, _key.Span, output.AsSpan(0, plaintextLength), out var written)
            || written != plaintextLength)
            throw new ZeroMqProtocolException("CURVE frame authentication failed");
        var flags = (ZmtpFrameFlags)output[0];
        if ((flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            flags == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid CURVE logical flags");
        _decodeNonce = tail;
        return new ZmtpFrameData { Flags = flags, Body = new ReadOnlySequence<byte>(output.AsMemory(1, plaintextLength - 1)) };
    }

    private static byte[] Ensure(ref byte[]? field, int length)
    {
        switch (field)
        {
            case { } existing when existing.Length >= length:
                return existing;
            case { } old:
                ArrayPool<byte>.Shared.Return(old, clearArray: true);
                break;
        }

        var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(256, length));
        field = buffer;
        return buffer;
    }

    private static void Return(byte[]? buffer)
    {
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _key = default;
        Return(_seal);
        Return(_plain);
        Return(_gathered);
        _seal = _plain = _gathered = null;
    }
}
