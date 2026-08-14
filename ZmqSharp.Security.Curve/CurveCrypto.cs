using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Math.EC.Rfc7748;
using Org.BouncyCastle.Security;

namespace ZmqSharp.Security.Curve;

/// <summary>
/// A 32-byte key stored inline as a value type (explicit layout, AOT-safe):
/// the CURVE long-term and per-connection ephemeral keys no longer cost two
/// heap arrays each. The struct and its fields are deliberately mutable: the
/// span views are built over the storage, and a readonly struct would force a
/// defensive copy on every field access, silently returning stale bytes
/// (0023).
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 32)]
public struct Key32 : IEquatable<Key32>
{
    private ulong w0;
    private ulong w1;
    private ulong w2;
    private ulong w3;

    /// <summary>The 32 key bytes as a read-only span, without a copy.</summary>
    public readonly ReadOnlySpan<byte> Span =>
        MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef(in w0), 4));

    /// <summary>Writable view for construction; never exposed publicly.</summary>
    private readonly Span<byte> WritableSpan =>
        MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref Unsafe.AsRef(in w0), 4));

    public static Key32 From(ReadOnlySpan<byte> source)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(source.Length, 32);
        var result = default(Key32);
        source.CopyTo(result.WritableSpan);
        return result;
    }

    public readonly void CopyTo(Span<byte> destination)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, 32);
        Span.CopyTo(destination);
    }

    public readonly bool Equals(Key32 other)
    {
        return Span.SequenceEqual(other.Span);
    }

    public override readonly bool Equals(object? obj)
    {
        return obj is Key32 other && Equals(other);
    }

    public override readonly int GetHashCode()
    {
        return HashCode.Combine(w0, w1, w2, w3);
    }

    public static bool operator ==(Key32 left, Key32 right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(Key32 left, Key32 right)
    {
        return !left.Equals(right);
    }
}

/// <summary>
/// The cryptographic primitives a CURVE (RFC 24 / CurveZMQ) mechanism needs.
/// The protocol skeleton (<see cref="CurveMechanism"/>) composes this; a user
/// supplies the backend with their library of choice - the BouncyCastle
/// implementation in this project is one pure-managed, AOT-safe option (0017).
/// Every primitive writes into a caller-provided destination (0023 D1): output
/// sizes are protocol-fixed, so the caller reserves the exact buffer and the
/// backend never allocates.
/// </summary>
public interface ICurveCryptoBackend
{
    /// <summary>Generates a fresh X25519 key pair (for the ephemeral connection keys).</summary>
    void GenerateKeyPair(out Key32 publicKey, out Key32 secretKey);

    /// <summary>crypto_box_beforenm: the NaCl box key, HSalsa20(X25519(...)), 32 bytes.</summary>
    void DeriveSharedSecret(ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination);

    /// <summary>
    /// crypto_box_curve25519xsalsa20poly1305: seals <paramref name="plaintext"/>
    /// with a 24-byte nonce into <paramref name="destination"/>, returning
    /// tag(16) + ciphertext; the return value is the number of bytes written.
    /// The box key is the X25519 shared secret between sender and recipient.
    /// </summary>
    int Box(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination);

    /// <summary>
    /// Opens a <see cref="Box"/> payload. The tag is verified first (0023 D5):
    /// on authentication failure the destination is left untouched and false is
    /// returned; on success <paramref name="written"/> is the plaintext length.
    /// </summary>
    bool TryUnbox(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> recipientSecret, ReadOnlySpan<byte> senderPublic,
        Span<byte> destination, out int written);

    /// <summary>XSalsa20-Poly1305 secretbox (used for the server cookie); returns the bytes written.</summary>
    int SecretBox(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination);

    /// <summary>Opens a <see cref="SecretBox"/> payload; tag verified before the destination is written.</summary>
    bool TrySecretBoxOpen(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination, out int written);

    /// <summary>Ed25519 signature over a message (64 bytes).</summary>
    void Sign(ReadOnlySpan<byte> message, ReadOnlySpan<byte> secretKey, Span<byte> signature);

    /// <summary>Verifies an Ed25519 signature over a message.</summary>
    bool Verify(ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature, ReadOnlySpan<byte> publicKey);

    /// <summary>CSPRNG output for ephemeral keys and nonce tails.</summary>
    void RandomBytes(Span<byte> destination);
}

/// <summary>
/// BouncyCastle-based <see cref="ICurveCryptoBackend"/> (0017 recommendation):
/// pure managed, no native dependency, IsAotCompatible. The crypto_box
/// construction composes XSalsa20 + Poly1305 the way libsodium's
/// crypto_secretbox does: the first 32 bytes of the XSalsa20 keystream form
/// the one-time Poly1305 key, the rest encrypts the message. The Salsa20 core
/// and Poly1305 are hand-written (0023) in the style of the existing HSalsa20,
/// so the hot path is stateless and allocates nothing; BouncyCastle is used
/// only for X25519 and Ed25519. libsodium known vectors lock the wire bytes.
/// </summary>
public sealed class BouncyCastleCurveCrypto : ICurveCryptoBackend
{
    public void GenerateKeyPair(out Key32 publicKey, out Key32 secretKey)
    {
        Span<byte> secret = stackalloc byte[32];
        RandomNumberGenerator.Fill(secret);
        Span<byte> pub = stackalloc byte[32];
        X25519.GeneratePublicKey(secret, pub);
        secretKey = Key32.From(secret);
        publicKey = Key32.From(pub);
    }

    /// <summary>
    /// X25519 shared secret, processed through HSalsa20 exactly as NaCl's
    /// crypto_box_beforenm does (the box key is not the raw X25519 output):
    /// 32 bytes.
    /// </summary>
    public void DeriveSharedSecret(ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("destination is too small for the shared secret", nameof(destination));

        Span<byte> x25519 = stackalloc byte[32];
        if (!X25519.CalculateAgreement(senderSecret, recipientPublic, x25519))
            throw new CryptographicException("X25519 agreement rejected the recipient public key");

        Span<byte> zeroInput = stackalloc byte[16];
        Hsalsa20(x25519, zeroInput, destination);
    }

    public int Box(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination)
    {
        Span<byte> boxKey = stackalloc byte[32];
        DeriveSharedSecret(senderSecret, recipientPublic, boxKey);
        return SecretBox(plaintext, nonce, boxKey, destination);
    }

    public bool TryUnbox(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> recipientSecret, ReadOnlySpan<byte> senderPublic,
        Span<byte> destination, out int written)
    {
        Span<byte> boxKey = stackalloc byte[32];
        DeriveSharedSecret(recipientSecret, senderPublic, boxKey);
        return TrySecretBoxOpen(boxed, nonce, boxKey, destination, out written);
    }

    public int SecretBox(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination)
    {
        var outputLength = 16 + plaintext.Length;
        if (destination.Length < outputLength)
            throw new ArgumentException("destination is too small for the sealed box", nameof(destination));

        XorWithKeystream(plaintext, destination[16..], key, nonce);

        Span<byte> polyKey = stackalloc byte[32];
        Poly1305Key(key, nonce, polyKey);
        Poly1305Tag(destination[16..outputLength], polyKey, destination[..16]);
        return outputLength;
    }

    public bool TrySecretBoxOpen(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination, out int written)
    {
        written = 0;
        if (boxed.Length < 16) return false;

        var ciphertext = boxed[16..];
        if (destination.Length < ciphertext.Length)
            throw new ArgumentException("destination is too small for the opened box", nameof(destination));

        // Verify the one-time Poly1305 tag before decrypting: the destination
        // is untouched on authentication failure (0023 D5).
        Span<byte> polyKey = stackalloc byte[32];
        Poly1305Key(key, nonce, polyKey);
        Span<byte> expected = stackalloc byte[16];
        Poly1305Tag(ciphertext, polyKey, expected);
        if (!CryptographicOperations.FixedTimeEquals(expected, boxed[..16])) return false;

        XorWithKeystream(ciphertext, destination[..ciphertext.Length], key, nonce);
        written = ciphertext.Length;
        return true;
    }

    public void Sign(ReadOnlySpan<byte> message, ReadOnlySpan<byte> secretKey, Span<byte> signature)
    {
        if (signature.Length < 64)
            throw new ArgumentException("destination is too small for the signature", nameof(signature));

        var signer = new Ed25519Signer();
        signer.Init(true, new Ed25519PrivateKeyParameters(secretKey));
        signer.BlockUpdate(message);
        signer.GenerateSignature().CopyTo(signature);
    }

    public bool Verify(ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature, ReadOnlySpan<byte> publicKey)
    {
        var verifier = new Ed25519Signer();
        verifier.Init(false, new Ed25519PublicKeyParameters(publicKey));
        verifier.BlockUpdate(message);
        return verifier.VerifySignature(signature.ToArray());
    }

    public void RandomBytes(Span<byte> destination)
    {
        RandomNumberGenerator.Fill(destination);
    }

    // ---- Salsa20 / HSalsa20 / XSalsa20 core ----

    /// <summary>Salsa20 quarterround (serial - each step consumes the previous).</summary>
    private static void QuarterRound(Span<uint> x, int a, int b, int c, int d)
    {
        var y0 = x[a];
        var y1 = x[b];
        var y2 = x[c];
        var y3 = x[d];
        var z1 = y1 ^ BitOperations.RotateLeft(y0 + y3, 7);
        var z2 = y2 ^ BitOperations.RotateLeft(z1 + y0, 9);
        var z3 = y3 ^ BitOperations.RotateLeft(z2 + z1, 13);
        var z0 = y0 ^ BitOperations.RotateLeft(z3 + z2, 18);
        x[a] = z0;
        x[b] = z1;
        x[c] = z2;
        x[d] = z3;
    }

    private static uint ReadLe32(ReadOnlySpan<byte> source, int offset)
    {
        return source[offset] | ((uint)source[offset + 1] << 8)
                              | ((uint)source[offset + 2] << 16) | ((uint)source[offset + 3] << 24);
    }

    private static void WriteLe32(Span<byte> target, int offset, uint value)
    {
        target[offset] = (byte)value;
        target[offset + 1] = (byte)(value >> 8);
        target[offset + 2] = (byte)(value >> 16);
        target[offset + 3] = (byte)(value >> 24);
    }

    /// <summary>
    /// HSalsa20 core: a 32-byte output from a 16-byte input and a 32-byte key.
    /// The crypto_box key is HSalsa20(X25519 shared secret, zero input); the
    /// XSalsa20 stream uses HSalsa20(key, nonce[..16]) as its subkey (0017).
    /// </summary>
    private static void Hsalsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> input, Span<byte> output)
    {
        // libsodium HSalsa20 (the NaCl crypto_box_beforenm construction):
        // interleaved state layout
        //   x0=sigma0, x1..x4=key[0..16), x5=sigma1, x6..x9=nonce,
        //   x10=sigma2, x11..x14=key[16..32), x15=sigma3
        // with serial Salsa20 quarterrounds (unlike ChaCha's parallel ones).
        Span<uint> x =
        [
            0x61707865,
            ReadLe32(key, 0), ReadLe32(key, 4),
            ReadLe32(key, 8), ReadLe32(key, 12),
            0x3320646e,
            ReadLe32(input, 0), ReadLe32(input, 4),
            ReadLe32(input, 8), ReadLe32(input, 12),
            0x79622d32,
            ReadLe32(key, 16), ReadLe32(key, 20),
            ReadLe32(key, 24), ReadLe32(key, 28),
            0x6b206574
        ];

        for (var i = 0; i < 10; i++)
        {
            // Column round.
            QuarterRound(x, 0, 4, 8, 12);
            QuarterRound(x, 5, 9, 13, 1);
            QuarterRound(x, 10, 14, 2, 6);
            QuarterRound(x, 15, 3, 7, 11);
            // Row round.
            QuarterRound(x, 0, 1, 2, 3);
            QuarterRound(x, 5, 6, 7, 4);
            QuarterRound(x, 10, 11, 8, 9);
            QuarterRound(x, 15, 12, 13, 14);
        }

        WriteLe32(output, 0, x[0]);
        WriteLe32(output, 4, x[5]);
        WriteLe32(output, 8, x[10]);
        WriteLe32(output, 12, x[15]);
        WriteLe32(output, 16, x[6]);
        WriteLe32(output, 20, x[7]);
        WriteLe32(output, 24, x[8]);
        WriteLe32(output, 28, x[9]);
    }

    /// <summary>Initial Salsa20 state for a 32-byte key, an 8-byte nonce, and a 64-bit block counter.</summary>
    private static void Salsa20State(ReadOnlySpan<byte> subkey, ReadOnlySpan<byte> nonce, ulong counter,
        Span<uint> state)
    {
        state[0] = 0x61707865;
        state[1] = ReadLe32(subkey, 0);
        state[2] = ReadLe32(subkey, 4);
        state[3] = ReadLe32(subkey, 8);
        state[4] = ReadLe32(subkey, 12);
        state[5] = 0x3320646e;
        state[6] = ReadLe32(nonce, 0);
        state[7] = ReadLe32(nonce, 4);
        state[8] = (uint)counter;
        state[9] = (uint)(counter >> 32);
        state[10] = 0x79622d32;
        state[11] = ReadLe32(subkey, 16);
        state[12] = ReadLe32(subkey, 20);
        state[13] = ReadLe32(subkey, 24);
        state[14] = ReadLe32(subkey, 28);
        state[15] = 0x6b206574;
    }

    /// <summary>
    /// One 64-byte Salsa20 keystream block from a state (output = state +
    /// working state, serialized in order - the libsodium salsa20 core); the
    /// state's counter is left untouched so the caller controls block
    /// sequencing.
    /// </summary>
    private static void Salsa20Block(Span<uint> state, Span<byte> output)
    {
        Span<uint> x = stackalloc uint[16];
        state.CopyTo(x);

        for (var i = 0; i < 10; i++)
        {
            QuarterRound(x, 0, 4, 8, 12);
            QuarterRound(x, 5, 9, 13, 1);
            QuarterRound(x, 10, 14, 2, 6);
            QuarterRound(x, 15, 3, 7, 11);
            QuarterRound(x, 0, 1, 2, 3);
            QuarterRound(x, 5, 6, 7, 4);
            QuarterRound(x, 10, 11, 8, 9);
            QuarterRound(x, 15, 12, 13, 14);
        }

        for (var i = 0; i < 16; i++) x[i] += state[i];

        for (var i = 0; i < 16; i++) WriteLe32(output, 4 * i, x[i]);
    }

    /// <summary>
    /// The XSalsa20 subkey: HSalsa20(key, nonce[..16]); the message stream then
    /// runs Salsa20(subkey, nonce[16..], counter). The first 32 bytes of the
    /// stream (block 0) are the one-time Poly1305 key.
    /// </summary>
    private static void Poly1305Key(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce, Span<byte> destination)
    {
        Span<byte> subkey = stackalloc byte[32];
        Hsalsa20(key, nonce[..16], subkey);

        Span<uint> state = stackalloc uint[16];
        Salsa20State(subkey, nonce[16..], 0, state);

        Span<byte> block = stackalloc byte[64];
        Salsa20Block(state, block);
        block[..32].CopyTo(destination);
    }

    /// <summary>
    /// XORs <paramref name="input"/> with the XSalsa20 keystream into
    /// <paramref name="output"/>, skipping the first 32 stream bytes (the
    /// one-time Poly1305 key) - the crypto_secretbox message stream.
    /// </summary>
    private static void XorWithKeystream(ReadOnlySpan<byte> input, Span<byte> output,
        ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce)
    {
        Span<byte> subkey = stackalloc byte[32];
        Hsalsa20(key, nonce[..16], subkey);

        Span<uint> state = stackalloc uint[16];
        Salsa20State(subkey, nonce[16..], 0, state);

        Span<byte> block = stackalloc byte[64];
        Salsa20Block(state, block);

        var pos = 0;
        var i = 32; // keystream byte offset within the block
        while (pos < input.Length)
        {
            output[pos] = (byte)(input[pos] ^ block[i]);
            pos++;
            i++;
            if (i == 64)
            {
                i = 0;
                state[8]++;
                Salsa20Block(state, block);
            }
        }
    }

    /// <summary>
    /// Poly1305 (RFC 8439): a 16-byte tag over the message under a 32-byte key.
    /// The reference 26-bit-limb arithmetic with 64-bit intermediates, kept
    /// allocation-free and stateless.
    /// </summary>
    private static void Poly1305Tag(ReadOnlySpan<byte> message, ReadOnlySpan<byte> key, Span<byte> tag)
    {
        const ulong mask = 0x3ffffff;

        // r = clamp(key[..16]).
        var t0 = ReadLe32(key, 0);
        var t1 = ReadLe32(key, 4);
        var r0 = t0 & 0x3ffffff;
        t0 >>= 26;
        t0 |= t1 << 6;
        var r1 = t0 & 0x3ffff03;
        t1 >>= 20;
        var t2 = ReadLe32(key, 8);
        t1 |= t2 << 12;
        var r2 = t1 & 0x3ffc0ff;
        t2 >>= 14;
        var t3 = ReadLe32(key, 12);
        t2 |= t3 << 18;
        var r3 = t2 & 0x3f03fff;
        t3 >>= 8;
        var r4 = t3 & 0x00fffff;

        var s1 = r1 * 5;
        var s2 = r2 * 5;
        var s3 = r3 * 5;
        var s4 = r4 * 5;

        ulong h0 = 0, h1 = 0, h2 = 0, h3 = 0, h4 = 0;

        var i = 0;
        while (message.Length - i >= 16)
        {
            h0 += ReadLe32(message, i) & mask;
            h1 += (ReadLe32(message, i + 3) >> 2) & mask;
            h2 += (ReadLe32(message, i + 6) >> 4) & mask;
            h3 += (ReadLe32(message, i + 9) >> 6) & mask;
            h4 += (ReadLe32(message, i + 12) >> 8) | (1UL << 24);
            Poly1305Multiply(ref h0, ref h1, ref h2, ref h3, ref h4, r0, r1, r2, r3, r4, s1, s2, s3, s4);
            i += 16;
        }

        // The final partial block: the remaining bytes plus the poly1305
        // terminator, a 1 bit at byte position `remaining`.
        if (i < message.Length)
        {
            ulong b0 = 0, b1 = 0, b2 = 0, b3 = 0;
            var remaining = message.Length - i;
            for (var j = 0; j < remaining; j++)
            {
                var word = j >> 2;
                var v = (ulong)message[i + j] << (8 * (j & 3));
                switch (word)
                {
                    case 0: b0 |= v; break;
                    case 1: b1 |= v; break;
                    case 2: b2 |= v; break;
                    default: b3 |= v; break;
                }
            }

            var termWord = remaining >> 2;
            var termShift = 8 * (remaining & 3);
            switch (termWord)
            {
                case 0: b0 |= 1UL << termShift; break;
                case 1: b1 |= 1UL << termShift; break;
                case 2: b2 |= 1UL << termShift; break;
                default: b3 |= 1UL << termShift; break;
            }

            h0 += b0 & mask;
            h1 += ((b0 >> 26) | (b1 << 6)) & mask;
            h2 += ((b1 >> 20) | (b2 << 12)) & mask;
            h3 += ((b2 >> 14) | (b3 << 18)) & mask;
            h4 += b3 >> 8;
            Poly1305Multiply(ref h0, ref h1, ref h2, ref h3, ref h4, r0, r1, r2, r3, r4, s1, s2, s3, s4);
        }

        // Fully carry h, then compute h + (2^130 - 5) and select the reduced
        // value (the reference poly1305 finish: g = h + -p, pick by borrow).
        ulong c = h1 >> 26; h1 &= mask; h2 += c;
        c = h2 >> 26; h2 &= mask; h3 += c;
        c = h3 >> 26; h3 &= mask; h4 += c;
        c = h4 >> 26; h4 &= mask; h0 += c * 5;
        c = h0 >> 26; h0 &= mask; h1 += c;

        ulong g0 = h0 + 5; c = g0 >> 26; g0 &= mask;
        ulong g1 = h1 + c; c = g1 >> 26; g1 &= mask;
        ulong g2 = h2 + c; c = g2 >> 26; g2 &= mask;
        ulong g3 = h3 + c; c = g3 >> 26; g3 &= mask;
        ulong g4 = h4 + c - (1UL << 26);
        g4 &= 0xffffffff; // the borrow test works on the low 32 bits

        var select = (g4 >> 31) - 1; // all ones when h >= p (pick h + -p), 0 when h < p
        g0 &= select;
        g1 &= select;
        g2 &= select;
        g3 &= select;
        g4 &= select;
        select = ~select;
        h0 = (h0 & select) | g0;
        h1 = (h1 & select) | g1;
        h2 = (h2 & select) | g2;
        h3 = (h3 & select) | g3;
        h4 = (h4 & select) | g4;

        // h % 2^128: reassemble the 26-bit limbs into 32-bit words.
        h0 = (h0 | (h1 << 26)) & 0xffffffff;
        h1 = ((h1 >> 6) | (h2 << 20)) & 0xffffffff;
        h2 = ((h2 >> 12) | (h3 << 14)) & 0xffffffff;
        h3 = (h3 >> 18) | (h4 << 8);

        // mac = (h + pad) % 2^128; the 2^128 carry is dropped.
        h0 += ReadLe32(key, 16);
        h1 += ReadLe32(key, 20) + (h0 >> 32); h0 &= 0xffffffff;
        h2 += ReadLe32(key, 24) + (h1 >> 32); h1 &= 0xffffffff;
        h3 += ReadLe32(key, 28) + (h2 >> 32); h2 &= 0xffffffff;
        h3 &= 0xffffffff;

        WriteLe32(tag, 0, (uint)h0);
        WriteLe32(tag, 4, (uint)h1);
        WriteLe32(tag, 8, (uint)h2);
        WriteLe32(tag, 12, (uint)h3);
    }

    /// <summary>h = h * r (mod 2^130 - 5), the poly1305 block multiply.</summary>
    private static void Poly1305Multiply(ref ulong h0, ref ulong h1, ref ulong h2, ref ulong h3, ref ulong h4,
        ulong r0, ulong r1, ulong r2, ulong r3, ulong r4,
        ulong s1, ulong s2, ulong s3, ulong s4)
    {
        const ulong mask = 0x3ffffff;

        var d0 = h0 * r0 + h1 * s4 + h2 * s3 + h3 * s2 + h4 * s1;
        var d1 = h0 * r1 + h1 * r0 + h2 * s4 + h3 * s3 + h4 * s2;
        var d2 = h0 * r2 + h1 * r1 + h2 * r0 + h3 * s4 + h4 * s3;
        var d3 = h0 * r3 + h1 * r2 + h2 * r1 + h3 * r0 + h4 * s4;
        var d4 = h0 * r4 + h1 * r3 + h2 * r2 + h3 * r1 + h4 * r0;

        ulong c = d0 >> 26; h0 = d0 & mask; d1 += c;
        c = d1 >> 26; h1 = d1 & mask; d2 += c;
        c = d2 >> 26; h2 = d2 & mask; d3 += c;
        c = d3 >> 26; h3 = d3 & mask; d4 += c;
        c = d4 >> 26; h4 = d4 & mask; h0 += c * 5;
        c = h0 >> 26; h0 &= mask; h1 += c;
    }
}
