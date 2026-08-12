using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Macs;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Math.EC.Rfc7748;
using Org.BouncyCastle.Security;

namespace ZmqSharp.Samples.Curve;

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

    /// <summary>X25519 shared secret (crypto_box_beforenm): 32 bytes.</summary>
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

    public byte[] DeriveSharedSecret(CurveKeyPair sender, byte[] recipientPublicKey)
    {
        var shared = new byte[32];
        if (!X25519.CalculateAgreement(sender.SecretKey, recipientPublicKey, shared))
            throw new CryptographicException("X25519 agreement rejected the recipient public key");

        return shared;
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
