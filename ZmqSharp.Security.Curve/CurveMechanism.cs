using System.Buffers.Binary;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve;

/// <summary>
/// The ZMTP CURVE mechanism (RFC 24 / CurveZMQ), written as an example against
/// ZmqSharp's public mechanism seam (0017): the handshake commands are sealed
/// boxes carried inside ordinary ZMTP command frames, driven through
/// <see cref="ZMechanismContext"/>, and the session returns a
/// <see cref="CurveSessionConnection"/> that encrypts traffic frames. The
/// protocol layout follows the maintained libzmq/NetMQ reference; only the
/// crypto primitives come from the user-supplied <see cref="ICurveCryptoBackend"/>.
/// </summary>
public sealed class CurveMechanism : IZSecurityMechanism
{
    private readonly ICurveCryptoBackend crypto;
    private readonly CurveKeyPair? clientLongTermKey;
    private readonly byte[]? serverPublicKey;
    private readonly CurveKeyPair? serverLongTermKey;

    /// <summary>Client role: authenticates with a long-term key pair against the server's public key.</summary>
    public CurveMechanism(ICurveCryptoBackend crypto, CurveKeyPair clientLongTermKey, byte[] serverPublicKey)
    {
        ArgumentNullException.ThrowIfNull(crypto);
        ArgumentNullException.ThrowIfNull(clientLongTermKey);
        ArgumentNullException.ThrowIfNull(serverPublicKey);
        if (serverPublicKey.Length != 32)
            throw new ArgumentOutOfRangeException(nameof(serverPublicKey), "server public key must be 32 bytes");

        this.crypto = crypto;
        this.clientLongTermKey = clientLongTermKey;
        this.serverPublicKey = serverPublicKey;
    }

    /// <summary>Server role: holds the long-term key pair the clients authenticate against.</summary>
    public CurveMechanism(ICurveCryptoBackend crypto, CurveKeyPair serverLongTermKey)
    {
        ArgumentNullException.ThrowIfNull(crypto);
        ArgumentNullException.ThrowIfNull(serverLongTermKey);
        this.crypto = crypto;
        this.serverLongTermKey = serverLongTermKey;
    }

    public string Name => "CURVE";

    public IZMechanismSession CreateSession(ZMechanismRole role)
    {
        if (role == ZMechanismRole.Client)
        {
            if (clientLongTermKey is null || serverPublicKey is null)
                throw new InvalidOperationException(
                    "a CURVE client requires a long-term key pair and the server public key");

            return new ClientSession(crypto, clientLongTermKey, serverPublicKey);
        }

        if (serverLongTermKey is null)
            throw new InvalidOperationException("a CURVE server requires a long-term key pair");

        return new ServerSession(crypto, serverLongTermKey);
    }

    private sealed class ClientSession(ICurveCryptoBackend crypto, CurveKeyPair longTerm, byte[] serverKey)
        : IZMechanismSession
    {
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            // Client handshake: HELLO -> WELCOME -> INITIATE -> READY.
            var ephemeral = crypto.GenerateKeyPair();
            var nonce = 1UL;

            var hello = BuildHello(ephemeral, serverKey, ref nonce);
            await context.WriteCommandAsync(hello, token);

            var welcome = await context.ReadCommandAsync(token);
            if (welcome is null) return null;
            if (welcome.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(welcome.Value));
            if (!welcome.Value.Name.Span.SequenceEqual("WELCOME"u8))
                throw new ZMechanismException("expected WELCOME after HELLO");

            var serverEphemeralKey = ParseWelcome(welcome.Value, ephemeral, serverKey, out var cookie);
            var sessionKey = crypto.DeriveSharedSecret(ephemeral, serverEphemeralKey);

            var initiate = BuildInitiate(context, longTerm, serverKey, ephemeral, serverEphemeralKey,
                sessionKey, cookie, ref nonce);
            await context.WriteCommandAsync(initiate, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(ready.Value));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after INITIATE");

            var peerMetadata = OpenReady(ready.Value, sessionKey);

            var session = new CurveSessionConnection(
                context.Connection, crypto, sessionKey,
                CurveConstants.MessagePrefixClientToServer, CurveConstants.MessagePrefixServerToClient,
                nonce, 0);
            return new ZMechanismResult(session, peerMetadata);
        }

        private byte[] BuildHello(CurveKeyPair ephemeral, byte[] serverKey, ref ulong nonce)
        {
            var body = new byte[200];
            CurveConstants.HelloLiteral.CopyTo(body, 0);
            body[6] = 1; // CurveZMQ major version
            body[7] = 0; // minor version
            // 8..80: zero padding (anti-amplification).
            ephemeral.PublicKey.CopyTo(body, 80);

            var fullNonce = Nonce(CurveConstants.HelloNoncePrefix, ref nonce);
            fullNonce.AsSpan(16).CopyTo(body.AsSpan(112));

            // Box of 64 zero bytes under (C', S): proves the client knows c'.
            var box = crypto.Box(new byte[64], fullNonce, ephemeral, serverKey);
            box.CopyTo(body, 120);
            return body;
        }

        private byte[] ParseWelcome(
            ZMechanismCommand welcome,
            CurveKeyPair ephemeral,
            byte[] serverKey,
            out byte[] cookie)
        {
            var args = welcome.Arguments;
            if (args.Length != 160) throw new ZMechanismException("malformed WELCOME command");

            var nonce = new byte[24];
            CurveConstants.WelcomeNoncePrefix.CopyTo(nonce, 0);
            args[..16].CopyTo(nonce.AsMemory(8));

            var plaintext = crypto.Unbox(args[16..].Span, nonce, ephemeral, serverKey)
                            ?? throw new ZMechanismException("WELCOME authentication failed");
            if (plaintext.Length != 128) throw new ZMechanismException("malformed WELCOME payload");

            cookie = plaintext[32..128];
            return plaintext[..32];
        }

        private byte[] BuildInitiate(
            ZMechanismContext context,
            CurveKeyPair longTerm,
            byte[] serverKey,
            CurveKeyPair ephemeral,
            byte[] serverEphemeralKey,
            byte[] sessionKey,
            byte[] cookie,
            ref ulong nonce)
        {
            // Vouch: Box [C', S](c -> S'), proves the long-term secret's owner
            // vouches for this connection's ephemeral key C'.
            var vouchPlain = new byte[64];
            ephemeral.PublicKey.CopyTo(vouchPlain, 0);
            serverKey.CopyTo(vouchPlain, 32);
            var vouchNonce = new byte[24];
            CurveConstants.VouchNoncePrefix.CopyTo(vouchNonce, 0);
            crypto.RandomBytes(16).CopyTo(vouchNonce, 8);
            var vouchBox = crypto.Box(vouchPlain, vouchNonce, longTerm, serverEphemeralKey);

            // Initiate: Box [C, vouch-nonce, vouch, metadata](C' -> S').
            var metadata = ReadyMetadata(context.LocalReadyBody);
            var initiatePlain = new byte[32 + 16 + 80 + metadata.Length];
            longTerm.PublicKey.CopyTo(initiatePlain, 0);
            vouchNonce.AsSpan(8).CopyTo(initiatePlain.AsSpan(32));
            vouchBox.CopyTo(initiatePlain, 48);
            metadata.Span.CopyTo(initiatePlain.AsSpan(128));

            var initiateNonce = Nonce(CurveConstants.InitiateNoncePrefix, ref nonce);
            var box = crypto.Box(initiatePlain, initiateNonce, ephemeral, serverEphemeralKey);

            var body = new byte[9 + cookie.Length + 8 + box.Length];
            CurveConstants.InitiateLiteral.CopyTo(body, 0);
            cookie.CopyTo(body, 9);
            initiateNonce.AsSpan(16).CopyTo(body.AsSpan(105));
            box.CopyTo(body, 113);
            return body;
        }

        private byte[] OpenReady(ZMechanismCommand ready, byte[] sessionKey)
        {
            var args = ready.Arguments;
            if (args.Length < 24) throw new ZMechanismException("malformed READY command");

            var nonce = new byte[24];
            CurveConstants.ReadyNoncePrefix.CopyTo(nonce, 0);
            args[..8].CopyTo(nonce.AsMemory(16));

            return crypto.SecretBoxOpen(args[8..].Span, nonce, sessionKey)
                   ?? throw new ZMechanismException("READY authentication failed");
        }

        private static string PeerError(ZMechanismCommand command)
        {
            return $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(command.Arguments.Span)}";
        }
    }

    private sealed class ServerSession(ICurveCryptoBackend crypto, CurveKeyPair longTerm) : IZMechanismSession
    {
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            // Server handshake: HELLO -> WELCOME -> INITIATE -> READY.
            var hello = await context.ReadCommandAsync(token);
            if (hello is null) return null;
            if (hello.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(hello.Value));
            if (!hello.Value.Name.Span.SequenceEqual("HELLO"u8))
                throw new ZMechanismException("expected HELLO");

            var clientEphemeralKey = ParseHello(hello.Value, longTerm, out var peerNonce);

            var ephemeral = crypto.GenerateKeyPair();
            var cookieKey = crypto.RandomBytes(32);
            var welcome = BuildWelcome(longTerm, ephemeral, clientEphemeralKey, cookieKey);
            await context.WriteCommandAsync(welcome, token);

            var initiate = await context.ReadCommandAsync(token);
            if (initiate is null) return null;
            if (initiate.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(initiate.Value));
            if (!initiate.Value.Name.Span.SequenceEqual("INITIATE"u8))
                throw new ZMechanismException("expected INITIATE after WELCOME");

            var peerMetadata = ParseInitiate(
                initiate.Value, longTerm, ephemeral, clientEphemeralKey, cookieKey, out var sessionKey);

            var nonce = 1UL;
            var ready = BuildReady(context, ephemeral, clientEphemeralKey, sessionKey, ref nonce);
            await context.WriteCommandAsync(ready, token);

            var session = new CurveSessionConnection(
                context.Connection, crypto, sessionKey,
                CurveConstants.MessagePrefixServerToClient, CurveConstants.MessagePrefixClientToServer,
                nonce, peerNonce);
            return new ZMechanismResult(session, peerMetadata);
        }

        private byte[] ParseHello(ZMechanismCommand hello, CurveKeyPair longTerm, out ulong peerNonce)
        {
            // HELLO is 200 bytes; reconstruct the full body so the fixed offsets match the reference.
            var body = new byte[6 + hello.Arguments.Length];
            CurveConstants.HelloLiteral.CopyTo(body, 0);
            hello.Arguments.Span.CopyTo(body.AsSpan(6));
            if (body.Length != 200) throw new ZMechanismException("malformed HELLO command");
            if (body[6] != 1 || body[7] != 0) throw new ZMechanismException("unsupported CURVE version");

            var clientEphemeralKey = body[80..112];
            peerNonce = BinaryPrimitives.ReadUInt64BigEndian(body.AsSpan(112, 8));

            var nonce = new byte[24];
            CurveConstants.HelloNoncePrefix.CopyTo(nonce, 0);
            body.AsSpan(112, 8).CopyTo(nonce.AsSpan(16));

            // The box must open to 64 zero bytes under (s, C').
            var plaintext = crypto.Unbox(body.AsSpan(120, 80), nonce, longTerm, clientEphemeralKey)
                            ?? throw new ZMechanismException("HELLO authentication failed");
            if (plaintext.Length != 64 || plaintext.Any(b => b != 0))
                throw new ZMechanismException("malformed HELLO payload");

            return clientEphemeralKey;
        }

        private byte[] BuildWelcome(
            CurveKeyPair longTerm,
            CurveKeyPair ephemeral,
            byte[] clientEphemeralKey,
            byte[] cookieKey)
        {
            // Cookie: SecretBox [C', s'](t); seals the server's ephemeral secret
            // so the server can verify INITIATE without keeping per-connection state.
            var cookiePlain = new byte[64];
            clientEphemeralKey.CopyTo(cookiePlain, 0);
            ephemeral.SecretKey.CopyTo(cookiePlain, 32);
            var cookieNonce = new byte[24];
            CurveConstants.CookieNoncePrefix.CopyTo(cookieNonce, 0);
            crypto.RandomBytes(16).CopyTo(cookieNonce, 8);
            var cookieBox = crypto.SecretBox(cookiePlain, cookieNonce, cookieKey);

            // Welcome: Box [S', cookie](s -> C').
            var welcomePlain = new byte[128];
            ephemeral.PublicKey.CopyTo(welcomePlain, 0);
            cookieNonce.AsSpan(8).CopyTo(welcomePlain.AsSpan(32));
            cookieBox.CopyTo(welcomePlain, 48);
            var welcomeNonce = new byte[24];
            CurveConstants.WelcomeNoncePrefix.CopyTo(welcomeNonce, 0);
            crypto.RandomBytes(16).CopyTo(welcomeNonce, 8);
            var welcomeBox = crypto.Box(welcomePlain, welcomeNonce, longTerm, clientEphemeralKey);

            var body = new byte[8 + 16 + welcomeBox.Length];
            CurveConstants.WelcomeLiteral.CopyTo(body, 0);
            welcomeNonce.AsSpan(8).CopyTo(body.AsSpan(8));
            welcomeBox.CopyTo(body, 24);
            return body;
        }

        private byte[] ParseInitiate(
            ZMechanismCommand initiate,
            CurveKeyPair longTerm,
            CurveKeyPair ephemeral,
            byte[] clientEphemeralKey,
            byte[] cookieKey,
            out byte[] sessionKey)
        {
            // Reconstruct the full body so the fixed offsets match the reference.
            var body = new byte[9 + initiate.Arguments.Length];
            CurveConstants.InitiateLiteral.CopyTo(body, 0);
            initiate.Arguments.Span.CopyTo(body.AsSpan(9));
            if (body.Length < 257) throw new ZMechanismException("malformed INITIATE command");

            // Open and verify the cookie (the C' + s' we sealed in WELCOME).
            var cookieNonce = new byte[24];
            CurveConstants.CookieNoncePrefix.CopyTo(cookieNonce, 0);
            body.AsSpan(9, 16).CopyTo(cookieNonce.AsSpan(8));
            var cookiePlain = crypto.SecretBoxOpen(body.AsSpan(25, 80), cookieNonce, cookieKey)
                              ?? throw new ZMechanismException("cookie authentication failed");
            if (!cookiePlain.AsSpan(0, 32).SequenceEqual(clientEphemeralKey)
                || !cookiePlain.AsSpan(32, 32).SequenceEqual(ephemeral.SecretKey))
                throw new ZMechanismException("cookie does not match the connection");

            // Open the initiate box under (s', C').
            var initiateNonce = new byte[24];
            CurveConstants.InitiateNoncePrefix.CopyTo(initiateNonce, 0);
            body.AsSpan(105, 8).CopyTo(initiateNonce.AsSpan(16));
            var initiatePlain = crypto.Unbox(body.AsSpan(113), initiateNonce, ephemeral, clientEphemeralKey)
                                ?? throw new ZMechanismException("INITIATE authentication failed");

            // Verify the vouch: Box [C, S](c -> S'), opened under (s', C).
            var clientLongKey = initiatePlain[..32];
            var vouchNonce = new byte[24];
            CurveConstants.VouchNoncePrefix.CopyTo(vouchNonce, 0);
            initiatePlain.AsSpan(32, 16).CopyTo(vouchNonce.AsSpan(8));
            var vouchPlain = crypto.Unbox(initiatePlain.AsSpan(48, 80), vouchNonce, ephemeral, clientLongKey)
                             ?? throw new ZMechanismException("vouch authentication failed");
            if (!vouchPlain.AsSpan(0, 32).SequenceEqual(clientEphemeralKey))
                throw new ZMechanismException("vouch does not match the connection");

            sessionKey = crypto.DeriveSharedSecret(ephemeral, clientEphemeralKey);
            return initiatePlain[128..];
        }

        private byte[] BuildReady(
            ZMechanismContext context,
            CurveKeyPair ephemeral,
            byte[] clientEphemeralKey,
            byte[] sessionKey,
            ref ulong nonce)
        {
            var metadata = ReadyMetadata(context.LocalReadyBody);
            var readyNonce = Nonce(CurveConstants.ReadyNoncePrefix, ref nonce);
            var box = crypto.Box(metadata.Span, readyNonce, ephemeral, clientEphemeralKey);

            var body = new byte[6 + 8 + box.Length];
            CurveConstants.ReadyLiteral.CopyTo(body, 0);
            readyNonce.AsSpan(16).CopyTo(body.AsSpan(6));
            box.CopyTo(body, 14);
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
    private static byte[] Nonce(byte[] prefix, ref ulong nonce)
    {
        var full = new byte[24];
        prefix.CopyTo(full, 0);
        BinaryPrimitives.WriteUInt64BigEndian(full.AsSpan(16), nonce);
        nonce++;
        return full;
    }
}

/// <summary>Fixed RFC 24 literals and nonce prefixes.</summary>
internal static class CurveConstants
{
    public static readonly byte[] HelloLiteral = [0x05, .. "HELLO"u8];
    public static readonly byte[] WelcomeLiteral = [0x07, .. "WELCOME"u8];
    public static readonly byte[] InitiateLiteral = [0x08, .. "INITIATE"u8];
    public static readonly byte[] ReadyLiteral = [0x05, .. "READY"u8];
    public static readonly byte[] MessageLiteral = [0x07, .. "MESSAGE"u8];

    public static readonly byte[] HelloNoncePrefix = [.. "CurveZMQHELLO---"u8];
    public static readonly byte[] WelcomeNoncePrefix = [.. "WELCOME-"u8];
    public static readonly byte[] VouchNoncePrefix = [.. "VOUCH---"u8];
    public static readonly byte[] InitiateNoncePrefix = [.. "CurveZMQINITIATE"u8];
    public static readonly byte[] ReadyNoncePrefix = [.. "CurveZMQREADY---"u8];
    public static readonly byte[] CookieNoncePrefix = [.. "COOKIE--"u8];
    public static readonly byte[] MessagePrefixClientToServer = [.. "CurveZMQMESSAGEC"u8];
    public static readonly byte[] MessagePrefixServerToClient = [.. "CurveZMQMESSAGES"u8];
}
