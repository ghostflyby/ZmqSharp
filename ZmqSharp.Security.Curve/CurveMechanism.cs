using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve;

/// <summary>
/// The ZMTP CURVE mechanism (RFC 25 / CurveZMQ), written as an example against
/// ZmqSharp's public mechanism seam (0017): the handshake commands are sealed
/// boxes carried inside ordinary ZMTP command frames, driven through
/// <see cref="ZMechanismContext"/>, and the session returns a
/// <see cref="CurveFrameCodec"/> that encrypts traffic frames. The
/// protocol layout follows the maintained libzmq/NetMQ reference; only the
/// crypto primitives come from the internal <see cref="CurveCrypto"/> primitives.
/// The handshake builds every fixed-size stage buffer with stackalloc and the
/// destination-style backend, so it allocates only the command frames
/// themselves (0027).
/// </summary>
public sealed class CurveMechanism : IZSecurityMechanism
{
    private readonly Key32? clientLongTermKey;
    private readonly Key32? serverPublicKey;
    private readonly Key32? serverLongTermKey;

    /// <summary>
    /// Client role: authenticates with a long-term key pair against the
    /// server's public key. Both keys must be 32 bytes and are copied at
    /// construction; later mutation of the inputs has no effect.
    /// </summary>
    public CurveMechanism(ReadOnlySpan<byte> clientLongTermKey, ReadOnlySpan<byte> serverPublicKey)
    {
        this.clientLongTermKey = Key32.From(clientLongTermKey);
        this.serverPublicKey = Key32.From(serverPublicKey);
    }

    /// <summary>
    /// Server role: holds the long-term secret key the clients authenticate
    /// against. The key must be 32 bytes and is copied at construction; later
    /// mutation of the input has no effect.
    /// </summary>
    public CurveMechanism(ReadOnlySpan<byte> serverLongTermKey)
    {
        this.serverLongTermKey = Key32.From(serverLongTermKey);
    }

    public string Name => "CURVE";

    public ZMechanismRole Role => serverLongTermKey is null ? ZMechanismRole.Client : ZMechanismRole.Server;

    public IZMechanismSession CreateSession()
    {
        if (Role == ZMechanismRole.Client)
        {
            if (clientLongTermKey is not { } clientKey || serverPublicKey is not { } serverKey)
                throw new InvalidOperationException(
                    "a CURVE client requires a long-term key pair and the server public key");

            // The long-term public key is derived from the secret; the wire
            // (INITIATE) carries the public, the vouch box is sealed with the
            // secret.
            Span<byte> publicBytes = stackalloc byte[32];
            CurveCrypto.DerivePublicKey(clientKey.Span, publicBytes);
            return new ClientSession(clientKey, Key32.From(publicBytes), serverKey);
        }

        return serverLongTermKey is { } serverSecretKey
            ? new ServerSession(serverSecretKey)
            : throw new InvalidOperationException("a CURVE server requires a long-term key pair");
    }

    private sealed class ClientSession(
        Key32 longTerm,
        Key32 longTermPublic,
        Key32 serverKey)
        : IZMechanismSession
    {
        // Handshake helpers are instance methods closing over the
        // primary-constructor keys directly; only per-handshake values
        // (ephemeral keys, cookie, nonce) travel as parameters.
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            // Client handshake: HELLO -> WELCOME -> INITIATE -> READY.
            CurveCrypto.GenerateKeyPair(out var ephemeralPublic, out var ephemeralSecret);
            var nonce = 1UL;

            var hello = BuildHello(ephemeralPublic, ephemeralSecret, ref nonce);
            await context.WriteCommandAsync(hello, token);

            var welcome = await context.ReadCommandAsync(token);
            if (welcome is null) return null;
            if (welcome.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(welcome.Value));
            if (!welcome.Value.Name.Span.SequenceEqual("WELCOME"u8))
                throw new ZMechanismException("expected WELCOME after HELLO");

            var serverEphemeralKey = ParseWelcome(welcome.Value, ephemeralSecret, out var cookie);

            Span<byte> sessionKeyBytes = stackalloc byte[32];
            CurveCrypto.DeriveSharedSecret(ephemeralSecret.Span, serverEphemeralKey.Span, sessionKeyBytes);
            var sessionKey = Key32.From(sessionKeyBytes);

            var initiate = BuildInitiate(context, ephemeralPublic, ephemeralSecret, serverEphemeralKey, cookie, ref nonce);
            await context.WriteCommandAsync(initiate, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(ready.Value));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after INITIATE");

            var peerMetadata = OpenReady(ready.Value, sessionKey);

            var session = new CurveFrameCodec(
                sessionKey,
                encodeServerToClient: false, decodeServerToClient: true,
                nonce, 0);
            return new ZMechanismResult(session, peerMetadata);
        }

        private byte[] BuildHello(Key32 ephemeralPublic, Key32 ephemeralSecret, ref ulong nonce)
        {
            var body = new byte[200];
            CurveConstants.HelloLiteral.CopyTo(body);
            body[6] = 1; // CurveZMQ major version
            body[7] = 0; // minor version
            // 8..80: zero padding (anti-amplification).
            ephemeralPublic.CopyTo(body.AsSpan(80));

            Span<byte> fullNonce = stackalloc byte[24];
            WriteNonce(CurveConstants.HelloNoncePrefix, ref nonce, fullNonce);
            fullNonce[16..].CopyTo(body.AsSpan(112));

            // Box of 64 zero bytes under (C', S): proves the client knows c'.
            Span<byte> zeroPlain = stackalloc byte[64];
            CurveCrypto.Box(zeroPlain, fullNonce, ephemeralSecret.Span, serverKey.Span, body.AsSpan(120));
            return body;
        }

        private Key32 ParseWelcome(
            ZMechanismCommand welcome,
            Key32 ephemeralSecret,
            out byte[] cookie)
        {
            var args = welcome.Arguments;
            if (args.Length != 160) throw new ZMechanismException("malformed WELCOME command");

            Span<byte> nonce = stackalloc byte[24];
            CurveConstants.WelcomeNoncePrefix.CopyTo(nonce);
            args[..16].Span.CopyTo(nonce[8..]);

            Span<byte> plaintext = stackalloc byte[128];
            if (!CurveCrypto.TryUnbox(args[16..].Span, nonce, ephemeralSecret.Span, serverKey.Span,
                    plaintext, out var written)
                || written != 128)
                throw new ZMechanismException("WELCOME authentication failed");

            var serverEphemeral = Key32.From(plaintext[..32]);
            cookie = [.. plaintext[32..128]];
            return serverEphemeral;
        }

        private byte[] BuildInitiate(
            ZMechanismContext context,
            Key32 ephemeralPublic,
            Key32 ephemeralSecret,
            Key32 serverEphemeralKey,
            byte[] cookie,
            ref ulong nonce)
        {
            // Vouch: Box [C', S](c -> S'), proves the long-term secret's owner
            // vouches for this connection's ephemeral key C'.
            Span<byte> vouchPlain = stackalloc byte[64];
            ephemeralPublic.CopyTo(vouchPlain[..32]);
            serverKey.CopyTo(vouchPlain[32..]);
            Span<byte> vouchNonce = stackalloc byte[24];
            CurveConstants.VouchNoncePrefix.CopyTo(vouchNonce);
            CurveCrypto.RandomBytes(vouchNonce[8..24]); // the full 16-byte nonce tail
            Span<byte> vouchBox = stackalloc byte[80];
            CurveCrypto.Box(vouchPlain, vouchNonce, longTerm.Span, serverEphemeralKey.Span, vouchBox);

            // Initiate: Box [C, vouch-nonce, vouch, metadata](C' -> S').
            var metadata = ReadyMetadata(context.LocalReadyBody);
            var initiatePlainLength = 32 + 16 + 80 + metadata.Length;
            Span<byte> initiatePlain = stackalloc byte[initiatePlainLength];
            longTermPublic.CopyTo(initiatePlain[..32]);
            vouchNonce[8..24].CopyTo(initiatePlain[32..48]);
            vouchBox.CopyTo(initiatePlain[48..128]);
            metadata.Span.CopyTo(initiatePlain[128..]);

            Span<byte> initiateNonce = stackalloc byte[24];
            WriteNonce(CurveConstants.InitiateNoncePrefix, ref nonce, initiateNonce);

            var body = new byte[9 + cookie.Length + 8 + 16 + initiatePlainLength];
            CurveConstants.InitiateLiteral.CopyTo(body);
            cookie.CopyTo(body, 9);
            initiateNonce[16..].CopyTo(body.AsSpan(105));
            CurveCrypto.Box(initiatePlain, initiateNonce, ephemeralSecret.Span, serverEphemeralKey.Span,
                body.AsSpan(113));
            return body;
        }

        private byte[] OpenReady(ZMechanismCommand ready, Key32 sessionKey)
        {
            var args = ready.Arguments;
            if (args.Length < 24) throw new ZMechanismException("malformed READY command");

            Span<byte> nonce = stackalloc byte[24];
            CurveConstants.ReadyNoncePrefix.CopyTo(nonce);
            args[..8].Span.CopyTo(nonce[16..]);

            // The boxed metadata (peer READY arguments) is the owned copy the
            // driver hands to the socket layer. The buffer is peer-sized (the
            // READY command is unauthenticated at this point), so it is rented
            // from the shared pool instead of stackalloc'd (0027 C2).
            var plaintext = ArrayPool<byte>.Shared.Rent(args.Length - 24);
            try
            {
                return CurveCrypto.TrySecretBoxOpen(args[8..].Span, nonce, sessionKey.Span, plaintext, out var written)
                    ? [.. plaintext[..written]]
                    : throw new ZMechanismException("READY authentication failed");
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(plaintext);
            }
        }

        private static string PeerError(ZMechanismCommand command)
        {
            return $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(command.Arguments.Span)}";
        }
    }

    private sealed class ServerSession(Key32 longTerm) : IZMechanismSession
    {
        // Same convention as ClientSession: the primary-constructor long-term
        // key is referenced directly; per-handshake values travel as parameters.
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            // Server handshake: HELLO -> WELCOME -> INITIATE -> READY.
            var hello = await context.ReadCommandAsync(token);
            if (hello is null) return null;
            if (hello.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(hello.Value));
            if (!hello.Value.Name.Span.SequenceEqual("HELLO"u8))
                throw new ZMechanismException("expected HELLO");

            var clientEphemeralKey = ParseHello(hello.Value, out var peerNonce);

            CurveCrypto.GenerateKeyPair(out var ephemeralPublic, out var ephemeralSecret);
            Span<byte> cookieKeyBytes = stackalloc byte[32];
            CurveCrypto.RandomBytes(cookieKeyBytes);
            var cookieKey = Key32.From(cookieKeyBytes);
            var welcome = BuildWelcome(ephemeralPublic, ephemeralSecret, clientEphemeralKey, cookieKey);
            await context.WriteCommandAsync(welcome, token);

            var initiate = await context.ReadCommandAsync(token);
            if (initiate is null) return null;
            if (initiate.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(initiate.Value));
            if (!initiate.Value.Name.Span.SequenceEqual("INITIATE"u8))
                throw new ZMechanismException("expected INITIATE after WELCOME");

            var peerMetadata = ParseInitiate(
                initiate.Value, ephemeralSecret, clientEphemeralKey, cookieKey, out var sessionKey);

            var nonce = 1UL;
            var ready = BuildReady(context, ephemeralSecret, clientEphemeralKey, ref nonce);
            await context.WriteCommandAsync(ready, token);

            var session = new CurveFrameCodec(
                sessionKey,
                encodeServerToClient: true, decodeServerToClient: false,
                nonce, peerNonce);
            return new ZMechanismResult(session, peerMetadata);
        }

        private Key32 ParseHello(ZMechanismCommand hello, out ulong peerNonce)
        {
            // HELLO is 200 bytes; reconstruct the full body so the fixed offsets match the reference.
            var body = new byte[6 + hello.Arguments.Length];
            CurveConstants.HelloLiteral.CopyTo(body);
            hello.Arguments.Span.CopyTo(body.AsSpan(6));
            if (body.Length != 200) throw new ZMechanismException("malformed HELLO command");
            if (body[6] != 1 || body[7] != 0) throw new ZMechanismException("unsupported CURVE version");

            var clientEphemeralKey = Key32.From(body.AsSpan(80, 32));
            peerNonce = BinaryPrimitives.ReadUInt64BigEndian(body.AsSpan(112, 8));

            Span<byte> nonce = stackalloc byte[24];
            CurveConstants.HelloNoncePrefix.CopyTo(nonce);
            body.AsSpan(112, 8).CopyTo(nonce[16..]);

            // The box must open to 64 zero bytes under (s, C').
            Span<byte> plaintext = stackalloc byte[64];
            if (!CurveCrypto.TryUnbox(body.AsSpan(120, 80), nonce, longTerm.Span, clientEphemeralKey.Span,
                    plaintext, out var written)
                || written != 64
                || plaintext[..64].ContainsAnyExcept((byte)0))
                throw new ZMechanismException("HELLO authentication failed");

            return clientEphemeralKey;
        }

        private byte[] BuildWelcome(
            Key32 ephemeralPublic,
            Key32 ephemeralSecret,
            Key32 clientEphemeralKey,
            Key32 cookieKey)
        {
            // Cookie: SecretBox [C', s'](t); seals the server's ephemeral secret
            // so the server can verify INITIATE without keeping per-connection state.
            Span<byte> cookiePlain = stackalloc byte[64];
            clientEphemeralKey.CopyTo(cookiePlain[..32]);
            ephemeralSecret.CopyTo(cookiePlain[32..]);
            Span<byte> cookieNonce = stackalloc byte[24];
            CurveConstants.CookieNoncePrefix.CopyTo(cookieNonce);
            CurveCrypto.RandomBytes(cookieNonce[8..24]); // the full 16-byte nonce tail
            Span<byte> cookieBox = stackalloc byte[80];
            CurveCrypto.SecretBox(cookiePlain, cookieNonce, cookieKey.Span, cookieBox);

            // Welcome: Box [S', cookie](s -> C').
            Span<byte> welcomePlain = stackalloc byte[128];
            ephemeralPublic.CopyTo(welcomePlain[..32]);
            cookieNonce[8..24].CopyTo(welcomePlain[32..48]);
            cookieBox.CopyTo(welcomePlain[48..128]);
            Span<byte> welcomeNonce = stackalloc byte[24];
            CurveConstants.WelcomeNoncePrefix.CopyTo(welcomeNonce);
            CurveCrypto.RandomBytes(welcomeNonce[8..24]); // the full 16-byte nonce tail
            Span<byte> welcomeBox = stackalloc byte[144];
            CurveCrypto.Box(welcomePlain, welcomeNonce, longTerm.Span, clientEphemeralKey.Span, welcomeBox);

            var body = new byte[8 + 16 + welcomeBox.Length];
            CurveConstants.WelcomeLiteral.CopyTo(body);
            welcomeNonce[8..24].CopyTo(body.AsSpan(8));
            welcomeBox.CopyTo(body.AsSpan(24));
            return body;
        }

        private byte[] ParseInitiate(
            ZMechanismCommand initiate,
            Key32 ephemeralSecret,
            Key32 clientEphemeralKey,
            Key32 cookieKey,
            out Key32 sessionKey)
        {
            // Reconstruct the full body so the fixed offsets match the reference.
            var body = new byte[9 + initiate.Arguments.Length];
            CurveConstants.InitiateLiteral.CopyTo(body);
            initiate.Arguments.Span.CopyTo(body.AsSpan(9));
            if (body.Length < 257) throw new ZMechanismException("malformed INITIATE command");

            // Open and verify the cookie (the C' + s' we sealed in WELCOME).
            Span<byte> cookieNonce = stackalloc byte[24];
            CurveConstants.CookieNoncePrefix.CopyTo(cookieNonce);
            body.AsSpan(9, 16).CopyTo(cookieNonce[8..]);
            Span<byte> cookiePlain = stackalloc byte[64];
            if (!CurveCrypto.TrySecretBoxOpen(body.AsSpan(25, 80), cookieNonce, cookieKey.Span, cookiePlain,
                    out var cookieWritten)
                || cookieWritten != 64
                || !cookiePlain[..32].SequenceEqual(clientEphemeralKey.Span)
                || !cookiePlain[32..].SequenceEqual(ephemeralSecret.Span))
                throw new ZMechanismException("cookie does not match the connection");

            // Open the initiate box under (s', C'). The plaintext length is
            // peer-sized (the INITIATE body is unauthenticated until the box
            // opens), so it is rented from the shared pool instead of
            // stackalloc'd (0027 C2).
            Span<byte> initiateNonce = stackalloc byte[24];
            CurveConstants.InitiateNoncePrefix.CopyTo(initiateNonce);
            body.AsSpan(105, 8).CopyTo(initiateNonce[16..]);
            var initiatePlain = ArrayPool<byte>.Shared.Rent(body.Length - 129);
            try
            {
                if (!CurveCrypto.TryUnbox(body.AsSpan(113), initiateNonce, ephemeralSecret.Span,
                        clientEphemeralKey.Span, initiatePlain, out var initiateWritten))
                    throw new ZMechanismException("INITIATE authentication failed");

                // Verify the vouch: Box [C, S](c -> S'), opened under (s', C).
                var clientLongKey = Key32.From(initiatePlain.AsSpan(0, 32));
                Span<byte> vouchNonce = stackalloc byte[24];
                CurveConstants.VouchNoncePrefix.CopyTo(vouchNonce);
                initiatePlain.AsSpan(32, 16).CopyTo(vouchNonce[8..]);
                Span<byte> vouchPlain = stackalloc byte[64];
                if (!CurveCrypto.TryUnbox(initiatePlain.AsSpan(48, 80), vouchNonce, ephemeralSecret.Span,
                        clientLongKey.Span, vouchPlain, out var vouchWritten)
                    || vouchWritten != 64
                    || !vouchPlain[..32].SequenceEqual(clientEphemeralKey.Span))
                    throw new ZMechanismException("vouch does not match the connection");

                Span<byte> sessionKeyBytes = stackalloc byte[32];
                CurveCrypto.DeriveSharedSecret(ephemeralSecret.Span, clientEphemeralKey.Span, sessionKeyBytes);
                sessionKey = Key32.From(sessionKeyBytes);
                return [.. initiatePlain[128..initiateWritten]];
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(initiatePlain);
            }
        }

        private byte[] BuildReady(
            ZMechanismContext context,
            Key32 ephemeralSecret,
            Key32 clientEphemeralKey,
            ref ulong nonce)
        {
            var metadata = ReadyMetadata(context.LocalReadyBody);
            Span<byte> readyNonce = stackalloc byte[24];
            WriteNonce(CurveConstants.ReadyNoncePrefix, ref nonce, readyNonce);
            Span<byte> box = stackalloc byte[16 + metadata.Length];
            CurveCrypto.Box(metadata.Span, readyNonce, ephemeralSecret.Span, clientEphemeralKey.Span, box);

            var body = new byte[6 + 8 + box.Length];
            CurveConstants.ReadyLiteral.CopyTo(body);
            readyNonce[16..].CopyTo(body.AsSpan(6));
            box.CopyTo(body.AsSpan(14));
            return body;
        }

        private static string PeerError(ZMechanismCommand command)
        {
            return $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(command.Arguments.Span)}";
        }
    }

    /// <summary>Extracts the metadata arguments (after the READY name) from the socket layer's READY body.</summary>
    private static ReadOnlyMemory<byte> ReadyMetadata(ReadOnlyMemory<byte> readyBody)
    {
        var span = readyBody.Span;
        if (!ZmtpCommandCodec.TryReadCommandName(span, out var name) || !name.SequenceEqual("READY"u8))
            throw new InvalidOperationException("expected a READY command body");

        return readyBody[(1 + name.Length)..];
    }

    /// <summary>Builds a full 24-byte nonce: 16-byte fixed prefix + 8-byte big-endian counter.</summary>
    private static void WriteNonce(ReadOnlySpan<byte> prefix, ref ulong nonce, Span<byte> destination)
    {
        prefix.CopyTo(destination);
        BinaryPrimitives.WriteUInt64BigEndian(destination[16..], nonce);
        nonce++;
    }
}

/// <summary>Fixed RFC 25 literals and nonce prefixes.</summary>
internal static class CurveConstants
{
    public static ReadOnlySpan<byte> HelloLiteral => "\u0005HELLO"u8;
    public static ReadOnlySpan<byte> WelcomeLiteral => "\aWELCOME"u8;
    public static ReadOnlySpan<byte> InitiateLiteral => "\bINITIATE"u8;
    public static ReadOnlySpan<byte> ReadyLiteral => "\u0005READY"u8;
    public static ReadOnlySpan<byte> MessageLiteral => "\aMESSAGE"u8;

    public static ReadOnlySpan<byte> HelloNoncePrefix => "CurveZMQHELLO---"u8;
    public static ReadOnlySpan<byte> WelcomeNoncePrefix => "WELCOME-"u8;
    public static ReadOnlySpan<byte> VouchNoncePrefix => "VOUCH---"u8;
    public static ReadOnlySpan<byte> InitiateNoncePrefix => "CurveZMQINITIATE"u8;
    public static ReadOnlySpan<byte> ReadyNoncePrefix => "CurveZMQREADY---"u8;
    public static ReadOnlySpan<byte> CookieNoncePrefix => "COOKIE--"u8;
    public static ReadOnlySpan<byte> MessagePrefixClientToServer => "CurveZMQMESSAGEC"u8;
    public static ReadOnlySpan<byte> MessagePrefixServerToClient => "CurveZMQMESSAGES"u8;
}
