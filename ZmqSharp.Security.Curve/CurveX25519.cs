using System.Security.Cryptography;
#if NET10_0_OR_GREATER && !NET11_0_OR_GREATER
using Org.BouncyCastle.Math.EC.Rfc7748;
#endif

namespace ZmqSharp.Security.Curve;

/// <summary>
/// The X25519 primitive behind <see cref="CurveCrypto"/>, selected per target
/// framework (0032): net11.0 uses the in-box
/// System.Security.Cryptography.X25519DiffieHellman, so the package carries no
/// crypto dependency on that target; net10.0 has no in-box X25519 and keeps
/// the BouncyCastle Rfc7748 implementation. Both paths reject a peer public
/// key whose shared secret is all zero, as libsodium's crypto_scalarmult
/// does. The in-box path is handshake-only and never touches the frame hot
/// path, which is entirely <see cref="CurveCrypto.SecretBox"/> under the
/// precomputed session key.
/// </summary>
internal static class CurveX25519
{
    private const string RejectedMessage = "X25519 agreement rejected the recipient public key";

    /// <summary>Derives the X25519 public key of a 32-byte secret key.</summary>
    public static void DerivePublicKey(ReadOnlySpan<byte> secretKey, Span<byte> publicKey)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(secretKey.Length, 32);
        ArgumentOutOfRangeException.ThrowIfLessThan(publicKey.Length, 32);
#if NET11_0_OR_GREATER
        using var key = X25519DiffieHellman.ImportPrivateKey(secretKey);
        key.ExportPublicKey(publicKey[..32]);
#else
        X25519.GeneratePublicKey(secretKey, publicKey);
#endif
    }

    /// <summary>
    /// Computes the raw X25519 shared secret. Throws
    /// <see cref="CryptographicException"/> when the peer public key is a
    /// low-order point: some providers (OpenSSL on macOS) reject it in the
    /// provider call, others may surface the all-zero secret, and the
    /// explicit check keeps every backend behaving like libsodium.
    /// </summary>
    public static void DeriveRawSecretAgreement(ReadOnlySpan<byte> secretKey, ReadOnlySpan<byte> publicKey,
        Span<byte> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(secretKey.Length, 32);
        ArgumentOutOfRangeException.ThrowIfNotEqual(publicKey.Length, 32);
        ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, 32);
#if NET11_0_OR_GREATER
        using var key = X25519DiffieHellman.ImportPrivateKey(secretKey);
        var secret = destination[..32];
        try
        {
            key.DeriveRawSecretAgreement(publicKey, secret);
        }
        catch (CryptographicException ex)
        {
            throw new CryptographicException(RejectedMessage, ex);
        }

        if (secret.IndexOfAnyExcept((byte)0) < 0) throw new CryptographicException(RejectedMessage);
#else
        if (!X25519.CalculateAgreement(secretKey, publicKey, destination))
            throw new CryptographicException(RejectedMessage);
#endif
    }
}
