using FluentAssertions;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

public sealed class CurveBackendBoundaryTests
{
    [Fact]
    public void PublicKeyDerivation_MatchesKnownVector()
    {
        var secret = Convert.FromHexString("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        var expected = Convert.FromHexString("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
        var publicKey = new byte[32];
        new BouncyCastleCurveCrypto().DerivePublicKey(secret, publicKey);
        publicKey.Should().Equal(expected);
    }

    [Fact]
    public void ClientSession_UsesSelectedBackendForPublicKeyDerivation()
    {
        var backend = new RecordingBackend();
        var secret = Key32.From(new byte[32]);
        var mechanism = new CurveMechanism(backend, secret, secret);
        mechanism.CreateSession().Should().NotBeNull();
        backend.Derivations.Should().Be(1);
    }

    private sealed class RecordingBackend : ICurveCryptoBackend
    {
        public int Derivations { get; private set; }
        public void DerivePublicKey(ReadOnlySpan<byte> secretKey, Span<byte> publicKey)
        {
            Derivations++;
            publicKey.Clear();
        }
        public void GenerateKeyPair(out Key32 publicKey, out Key32 secretKey) => throw new NotSupportedException();
        public void DeriveSharedSecret(ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic, Span<byte> destination) => throw new NotSupportedException();
        public int Box(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> senderSecret,
            ReadOnlySpan<byte> recipientPublic, Span<byte> destination) => throw new NotSupportedException();
        public bool TryUnbox(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> recipientSecret,
            ReadOnlySpan<byte> senderPublic, Span<byte> destination, out int written) => throw new NotSupportedException();
        public int SecretBox(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key, Span<byte> destination) => throw new NotSupportedException();
        public bool TrySecretBoxOpen(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key,
            Span<byte> destination, out int written) => throw new NotSupportedException();
        public void Sign(ReadOnlySpan<byte> message, ReadOnlySpan<byte> secretKey, Span<byte> signature) => throw new NotSupportedException();
        public bool Verify(ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature, ReadOnlySpan<byte> publicKey) => throw new NotSupportedException();
        public void RandomBytes(Span<byte> destination) => throw new NotSupportedException();
    }
}
