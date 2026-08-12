using FluentAssertions;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

/// <summary>
/// Byte-for-byte compatibility with libsodium (the crypto backend of libzmq's
/// CURVE), locked by known vectors generated with PyNaCl. The
/// crypto_box_beforenm key derivation (HSalsa20 of the X25519 shared secret)
/// is the critical piece - a raw-X25519 derivation silently produces
/// self-consistent but non-interoperable boxes.
/// </summary>
public sealed class LibsodiumKnownVectorTests
{
    private static readonly byte[] Sk1 =
        Convert.FromHexString("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");

    private static readonly byte[] Pk1 =
        Convert.FromHexString("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");

    private static readonly byte[] Sk2 =
        Convert.FromHexString("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");

    private static readonly byte[] Pk2 =
        Convert.FromHexString("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");

    private static readonly byte[] Nonce =
        Convert.FromHexString("df44535f814886b74ff0ff2ab0b8d2ff15b128594cabb60b");

    private static readonly byte[] Plain = [.. "cross box test"u8];

    private static readonly byte[] LibsodiumBox =
        Convert.FromHexString("2596569f8ea62536c778dc731a92ee70f4df000f54b57626e65c61fbad29");

    private static readonly byte[] LibsodiumBeforenm =
        Convert.FromHexString("1b27556473e985d462cd51197a9a46c76009549eac6474f206c4ee0844f68389");

    [Fact]
    public void DeriveSharedSecret_MatchesCryptoBoxBeforenm()
    {
        var bc = new BouncyCastleCurveCrypto();

        var derived = bc.DeriveSharedSecret(new CurveKeyPair(Pk1, Sk1), Pk2);
        derived.Should().Equal(LibsodiumBeforenm);

        // The derivation is symmetric, like X25519 itself.
        var reversed = bc.DeriveSharedSecret(new CurveKeyPair(Pk2, Sk2), Pk1);
        reversed.Should().Equal(LibsodiumBeforenm);
    }

    [Fact]
    public void Box_ProducesExactlyTheLibsodiumCiphertext()
    {
        var bc = new BouncyCastleCurveCrypto();

        var boxed = bc.Box(Plain, Nonce, new CurveKeyPair(Pk1, Sk1), Pk2);
        boxed.Should().Equal(LibsodiumBox);
    }

    [Fact]
    public void Unbox_OpensTheLibsodiumCiphertext()
    {
        var bc = new BouncyCastleCurveCrypto();

        var opened = bc.Unbox(LibsodiumBox, Nonce, new CurveKeyPair(Pk2, Sk2), Pk1);
        opened.Should().NotBeNull();
        opened.Should().Equal(Plain);
    }

    [Fact]
    public void Unbox_WithWrongRecipient_Fails()
    {
        var bc = new BouncyCastleCurveCrypto();
        var wrong = bc.GenerateKeyPair();

        bc.Unbox(LibsodiumBox, Nonce, wrong, Pk1).Should().BeNull();
    }

    [Fact]
    public void SecretBox_MatchesLibsodiumKnownVector()
    {
        var bc = new BouncyCastleCurveCrypto();
        var key = Convert.FromHexString("d1c816babcead3bacd134bfcef21bf4dd2d45e1409155c28f24be09a2147154e");
        var nonce = Convert.FromHexString("b05205a845db9a91e74358c4742652408315c3a3accfb849");
        var libsodium = Convert.FromHexString(
            "3a21ccb5a9a6b2fde7ed08bdd6a863d23cc41f4b3e536cebb1600e539fa2c2480b99c91523d0fa");

        var sealed_ = bc.SecretBox([.. "secret box test message"u8], nonce, key);
        sealed_.Should().Equal(libsodium);

        var opened = bc.SecretBoxOpen(libsodium, nonce, key);
        opened.Should().Equal([.. "secret box test message"u8]);
    }

    [Fact]
    public void GenerateKeyPair_MatchesScalarmultBase()
    {
        var bc = new BouncyCastleCurveCrypto();
        var kp = bc.GenerateKeyPair();
        kp.PublicKey.Should().HaveCount(32);
        kp.SecretKey.Should().HaveCount(32);
    }
}
