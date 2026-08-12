using System.Buffers.Binary;
using System.Numerics;
using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Macs;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Math.EC.Rfc7748;
using Org.BouncyCastle.Security;

namespace ZmqSharp.Security.Curve;

/// <summary>An X25519 key pair; keys are 32-byte big-endian scalars/points.</summary>
public sealed record CurveKeyPair(byte[] PublicKey, byte[] SecretKey);

/// <summary>
/// The cryptographic primitives a CURVE (RFC 24 / CurveZMQ) mechanism needs.
/// The protocol skeleton (<see cref="CurveMechanism"/>) composes this; a user
/// supplies the backend with their library of choice - the BouncyCastle
/// implementation in this project is one pure-managed, AOT-safe option (0017).
/// </summary>
public interface ICurveCryptoBackend
{
    /// <summary>Generates a fresh X25519 key pair (for the ephemeral connection keys).</summary>
    CurveKeyPair GenerateKeyPair();

    /// <summary>crypto_box_beforenm: the NaCl box key, HSalsa20(X25519(...)), 32 bytes.</summary>
    byte[] DeriveSharedSecret(CurveKeyPair sender, byte[] recipientPublicKey);

    /// <summary>
    /// crypto_box_curve25519xsalsa20poly1305: seals <paramref name="plaintext"/>
    /// with a 24-byte nonce, returning tag(16) + ciphertext. The box key is the
    /// X25519 shared secret between sender and recipient.
    /// </summary>
    byte[] Box(ReadOnlySpan<byte> plaintext, byte[] nonce, CurveKeyPair sender, byte[] recipientPublicKey);

    /// <summary>Opens a <see cref="Box"/> payload; returns null when authentication fails.</summary>
    byte[]? Unbox(ReadOnlySpan<byte> boxed, byte[] nonce, CurveKeyPair recipient, byte[] senderPublicKey);

    /// <summary>XSalsa20-Poly1305 secretbox (used for the server cookie).</summary>
    byte[] SecretBox(ReadOnlySpan<byte> plaintext, byte[] nonce, byte[] key);

    /// <summary>Opens a <see cref="SecretBox"/> payload; returns null when authentication fails.</summary>
    byte[]? SecretBoxOpen(ReadOnlySpan<byte> boxed, byte[] nonce, byte[] key);

    /// <summary>Ed25519 signature over a message (64 bytes).</summary>
    byte[] Sign(ReadOnlySpan<byte> message, byte[] secretKey);

    /// <summary>Verifies an Ed25519 signature over a message.</summary>
    bool Verify(ReadOnlySpan<byte> message, byte[] signature, byte[] publicKey);

    /// <summary>CSPRNG output for ephemeral keys and nonce tails.</summary>
    byte[] RandomBytes(int count);
}

/// <summary>
/// BouncyCastle-based <see cref="ICurveCryptoBackend"/> (0017 recommendation):
/// pure managed, no native dependency, IsAotCompatible. The crypto_box
/// construction composes BC's XSalsa20Engine + Poly1305 the way libsodium's
/// crypto_secretbox does: the first 32 bytes of the XSalsa20 keystream form
/// the one-time Poly1305 key, the rest encrypts the message.
/// </summary>
public sealed class BouncyCastleCurveCrypto : ICurveCryptoBackend
{
    public CurveKeyPair GenerateKeyPair()
    {
        var secret = new byte[32];
        X25519.GeneratePrivateKey(new SecureRandom(), secret);
        var pub = new byte[32];
        X25519.GeneratePublicKey(secret, pub);
        return new CurveKeyPair(pub, secret);
    }

    /// <summary>
    /// X25519 shared secret, processed through HSalsa20 exactly as NaCl's
    /// crypto_box_beforenm does (the box key is not the raw X25519 output):
    /// 32 bytes.
    /// </summary>
    public byte[] DeriveSharedSecret(CurveKeyPair sender, byte[] recipientPublicKey)
    {
        var x25519 = new byte[32];
        if (!X25519.CalculateAgreement(sender.SecretKey, recipientPublicKey, x25519))
            throw new CryptographicException("X25519 agreement rejected the recipient public key");

        return Hsalsa20(x25519);
    }

    private static byte[] Hsalsa20(byte[] x25519Shared)
    {
        // libsodium HSalsa20 (the NaCl crypto_box_beforenm construction):
        // interleaved state layout
        //   x0=sigma0, x1..x4=key[0..16), x5=sigma1, x6..x9=nonce,
        //   x10=sigma2, x11..x14=key[16..32), x15=sigma3
        // with serial Salsa20 quarterrounds (unlike ChaCha's parallel ones).
        // BouncyCastle exposes XSalsa20 but not the HSalsa20 core, so the core
        // is implemented here and locked to libsodium by the interop tests.
        Span<uint> x =
        [
            0x61707865,
            ReadLe32(x25519Shared, 0), ReadLe32(x25519Shared, 4),
            ReadLe32(x25519Shared, 8), ReadLe32(x25519Shared, 12),
            0x3320646e,
            0, 0, 0, 0, // zero nonce
            0x79622d32,
            ReadLe32(x25519Shared, 16), ReadLe32(x25519Shared, 20),
            ReadLe32(x25519Shared, 24), ReadLe32(x25519Shared, 28),
            0x6b206574,
        ];

        for (var i = 0; i < 10; i++)
        {
            // Column round.
            QuarterRound(x, 0, 4, 8, 12); QuarterRound(x, 5, 9, 13, 1);
            QuarterRound(x, 10, 14, 2, 6); QuarterRound(x, 15, 3, 7, 11);
            // Row round.
            QuarterRound(x, 0, 1, 2, 3); QuarterRound(x, 5, 6, 7, 4);
            QuarterRound(x, 10, 11, 8, 9); QuarterRound(x, 15, 12, 13, 14);
        }

        var output = new byte[32];
        WriteLe32(output, 0, x[0]);
        WriteLe32(output, 4, x[5]);
        WriteLe32(output, 8, x[10]);
        WriteLe32(output, 12, x[15]);
        WriteLe32(output, 16, x[6]);
        WriteLe32(output, 20, x[7]);
        WriteLe32(output, 24, x[8]);
        WriteLe32(output, 28, x[9]);
        return output;
    }

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

    private static uint ReadLe32(byte[] source, int offset)
    {
        return (uint)source[offset] | ((uint)source[offset + 1] << 8)
               | ((uint)source[offset + 2] << 16) | ((uint)source[offset + 3] << 24);
    }

    private static void WriteLe32(byte[] target, int offset, uint value)
    {
        target[offset] = (byte)value;
        target[offset + 1] = (byte)(value >> 8);
        target[offset + 2] = (byte)(value >> 16);
        target[offset + 3] = (byte)(value >> 24);
    }

    public byte[] Box(ReadOnlySpan<byte> plaintext, byte[] nonce, CurveKeyPair sender, byte[] recipientPublicKey)
    {
        return SecretBox(plaintext, nonce, DeriveSharedSecret(sender, recipientPublicKey));
    }

    public byte[]? Unbox(ReadOnlySpan<byte> boxed, byte[] nonce, CurveKeyPair recipient, byte[] senderPublicKey)
    {
        return SecretBoxOpen(boxed, nonce, DeriveSharedSecret(recipient, senderPublicKey));
    }

    public byte[] SecretBox(ReadOnlySpan<byte> plaintext, byte[] nonce, byte[] key)
    {
        // XSalsa20 keystream: block 0 (32 bytes) is the one-time Poly1305 key,
        // the remaining stream encrypts the message (libsodium crypto_secretbox).
        var keystream = GenerateKeystream(key, nonce, 32 + plaintext.Length);
        var polyKey = keystream[..32];
        var ciphertext = new byte[plaintext.Length];
        for (var i = 0; i < plaintext.Length; i++)
            ciphertext[i] = (byte)(plaintext[i] ^ keystream[32 + i]);

        var mac = Poly1305(ciphertext, polyKey);
        var result = new byte[16 + ciphertext.Length];
        mac.CopyTo(result, 0);
        ciphertext.CopyTo(result, 16);
        return result;
    }

    public byte[]? SecretBoxOpen(ReadOnlySpan<byte> boxed, byte[] nonce, byte[] key)
    {
        if (boxed.Length < 16) return null;

        var keystream = GenerateKeystream(key, nonce, boxed.Length - 16 + 32);
        var polyKey = keystream[..32];
        var expected = Poly1305(boxed[16..], polyKey);
        if (!CryptographicOperations.FixedTimeEquals(expected, boxed[..16])) return null;

        var plaintext = new byte[boxed.Length - 16];
        for (var i = 0; i < plaintext.Length; i++)
            plaintext[i] = (byte)(boxed[16 + i] ^ keystream[32 + i]);

        return plaintext;
    }

    public byte[] Sign(ReadOnlySpan<byte> message, byte[] secretKey)
    {
        var signer = new Ed25519Signer();
        signer.Init(true, new Ed25519PrivateKeyParameters(secretKey, 0));
        signer.BlockUpdate(message);
        return signer.GenerateSignature();
    }

    public bool Verify(ReadOnlySpan<byte> message, byte[] signature, byte[] publicKey)
    {
        var verifier = new Ed25519Signer();
        verifier.Init(false, new Ed25519PublicKeyParameters(publicKey, 0));
        verifier.BlockUpdate(message);
        return verifier.VerifySignature(signature);
    }

    public byte[] RandomBytes(int count)
    {
        return RandomNumberGenerator.GetBytes(count);
    }

    private static byte[] GenerateKeystream(byte[] key, byte[] nonce, int length)
    {
        var engine = new XSalsa20Engine();
        engine.Init(true, new ParametersWithIV(new KeyParameter(key), nonce));
        var keystream = new byte[length];
        engine.ProcessBytes(new byte[length], 0, length, keystream, 0);
        return keystream;
    }

    private static byte[] Poly1305(ReadOnlySpan<byte> message, byte[] key)
    {
        var mac = new Poly1305();
        mac.Init(new KeyParameter(key));
        mac.BlockUpdate(message);
        var tag = new byte[16];
        mac.DoFinal(tag, 0);
        return tag;
    }
}
