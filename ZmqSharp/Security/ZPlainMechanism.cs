using System.Text;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// The PLAIN security mechanism (RFC 24): the client sends HELLO with
/// length-prefixed credentials, the server authenticates and replies WELCOME,
/// then the client sends INITIATE metadata and the server sends READY. PLAIN is pure command frames - no
/// cryptography - and adds only authentication to the NULL exchange (0015
/// section 3.3).
///
/// This implementation deliberately uses only the public mechanism surface
/// (IZSecurityMechanism, ZMechanismContext, ZmtpCommandCodec, ZmtpCommands),
/// so a user could ship an equivalent mechanism without any library internals
/// (0016 section 3.1). The client role carries fixed credentials; the server
/// role carries an authenticator delegate.
/// </summary>
public sealed class ZPlainMechanism : IZSecurityMechanism
{
    private readonly string? username;
    private readonly ReadOnlyMemory<byte> password;
    private readonly ZPlainAuthenticator? authenticator;

    /// <summary>Client role: the credentials sent in every HELLO.</summary>
    public ZPlainMechanism(string username, ReadOnlySpan<byte> password)
    {
        ArgumentNullException.ThrowIfNull(username);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Encoding.UTF8.GetByteCount(username), byte.MaxValue, nameof(username));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(password.Length, byte.MaxValue, nameof(password));
        this.username = username;
        this.password = password.ToArray();
    }

    /// <summary>Server role: authenticates each connection's HELLO credentials.</summary>
    public ZPlainMechanism(ZPlainAuthenticator authenticator)
    {
        ArgumentNullException.ThrowIfNull(authenticator);
        this.authenticator = authenticator;
    }

    public string Name => "PLAIN";

    public ZMechanismRole Role => authenticator is null ? ZMechanismRole.Client : ZMechanismRole.Server;

    public IZMechanismSession CreateSession()
    {
        return new PlainSession(Role, username, password, authenticator);
    }

    private sealed class PlainSession(
        ZMechanismRole role,
        string? username,
        ReadOnlyMemory<byte> password,
        ZPlainAuthenticator? authenticator) : IZMechanismSession
    {
        private const string RejectionReason = "Invalid username or password";

        public ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            return role == ZMechanismRole.Client
                ? ClientAsync(context, token)
                : ServerAsync(context, token);
        }

        // Client: HELLO -> WELCOME -> INITIATE -> READY.
        private async ValueTask<ZMechanismResult?> ClientAsync(ZMechanismContext context, CancellationToken token)
        {
            await context.WriteCommandAsync(BuildHello(), token);

            var welcome = await context.ReadCommandAsync(token);
            if (welcome is null) return null;
            if (welcome.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(welcome.Value));
            if (!welcome.Value.Name.Span.SequenceEqual("WELCOME"u8))
                throw new ZMechanismException("expected WELCOME after HELLO");

            await context.WriteCommandAsync(BuildInitiate(context.LocalReadyBody), token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(ready.Value));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after WELCOME");

            return new ZMechanismResult(null, ready.Value.Arguments.ToArray());
        }

        // Server: HELLO -> authenticate -> WELCOME -> INITIATE -> READY. A rejected connection is answered with ERROR
        // before faulting, matching the RFC 23 error pattern.
        private async ValueTask<ZMechanismResult?> ServerAsync(ZMechanismContext context, CancellationToken token)
        {
            var hello = await context.ReadCommandAsync(token);
            if (hello is null) return null;
            if (hello.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(hello.Value));
            if (!hello.Value.Name.Span.SequenceEqual("HELLO"u8))
                throw new ZMechanismException("expected HELLO");

            if (!TryParseHello(hello.Value, out var user, out var peerPassword))
            {
                await context.WriteCommandAsync(ZmtpCommands.BuildError(RejectionReason), token);
                throw new ZMechanismException("HELLO is missing Username or Password");
            }

            if (authenticator is not { } authenticate || !authenticate(user, peerPassword))
            {
                await context.WriteCommandAsync(ZmtpCommands.BuildError(RejectionReason), token);
                throw new ZMechanismException(RejectionReason);
            }

            await context.WriteCommandAsync(BuildWelcome(), token);
            var initiate = await context.ReadCommandAsync(token);
            if (initiate is null) return null;
            if (initiate.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(initiate.Value));
            if (!initiate.Value.Name.Span.SequenceEqual("INITIATE"u8))
                throw new ZMechanismException("expected INITIATE after WELCOME");
            var metadata = initiate.Value.Arguments.ToArray();
            ZmtpCommandCodec.ParseReadySocketType(metadata);
            await context.WriteCommandAsync(context.LocalReadyBody, token);
            return new ZMechanismResult(null, metadata);
        }

        /// <summary>Reads the two one-byte-length credential fields, preserving password bytes.</summary>
        private static bool TryParseHello(ZMechanismCommand hello, out string username, out ReadOnlySpan<byte> password)
        {
            username = string.Empty;
            password = default;
            var body = hello.Arguments.Span;
            if (body.IsEmpty) return false;
            var userLength = body[0];
            if (body.Length < userLength + 2) return false;
            var passOffset = userLength + 1;
            var passLength = body[passOffset];
            if (body.Length != passOffset + 1 + passLength) return false;
            username = Encoding.UTF8.GetString(body.Slice(1, userLength));
            password = body.Slice(passOffset + 1, passLength);
            return true;
        }

        /// <summary>The peer's ERROR reason, or a protocol error if the ERROR body is malformed.</summary>
        private static string PeerError(ZMechanismCommand command)
        {
            return $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(command.Arguments.Span)}";
        }

        /// <summary>HELLO contains length-prefixed credentials, not metadata properties.</summary>
        private byte[] BuildHello()
        {
            var user = Encoding.UTF8.GetBytes(username is { } name
                ? name
                : throw new InvalidOperationException("a PLAIN client requires fixed credentials"));
            var body = new byte[8 + user.Length + password.Length];
            body[0] = 5;
            "HELLO"u8.CopyTo(body.AsSpan(1));
            body[6] = (byte)user.Length;
            user.CopyTo(body.AsSpan(7));
            body[7 + user.Length] = (byte)password.Length;
            password.Span.CopyTo(body.AsSpan(8 + user.Length));
            return body;
        }

        private static byte[] BuildInitiate(ReadOnlyMemory<byte> ready)
        {
            var metadata = ready.Span[6..];
            var body = new byte[9 + metadata.Length];
            body[0] = 8;
            "INITIATE"u8.CopyTo(body.AsSpan(1));
            metadata.CopyTo(body.AsSpan(9));
            return body;
        }

        /// <summary>WELCOME body: short-string name with no properties.</summary>
        private static byte[] BuildWelcome()
        {
            var body = new byte[8];
            body[0] = 7;
            "WELCOME"u8.CopyTo(body.AsSpan(1));
            return body;
        }
    }
}
