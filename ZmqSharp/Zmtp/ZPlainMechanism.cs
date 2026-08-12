using System.Text;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The PLAIN security mechanism (RFC 27): the client sends HELLO with
/// Username/Password metadata properties, the server authenticates and replies
/// WELCOME, then both sides exchange READY. PLAIN is pure command frames - no
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

    public IZMechanismSession CreateSession(ZMechanismRole role)
    {
        if (role == ZMechanismRole.Client && username is null)
            throw new InvalidOperationException("a PLAIN client requires fixed credentials");
        if (role == ZMechanismRole.Server && authenticator is null)
            throw new InvalidOperationException("a PLAIN server requires an authenticator");

        return new PlainSession(role, username, password, authenticator);
    }

    private sealed class PlainSession(ZMechanismRole role, string? username, ReadOnlyMemory<byte> password,
        ZPlainAuthenticator? authenticator) : IZMechanismSession
    {
        private const string RejectionReason = "Invalid username or password";

        public ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            return role == ZMechanismRole.Client
                ? ClientAsync(context, token)
                : ServerAsync(context, token);
        }

        // Client (as-server = 0): HELLO -> WELCOME -> local READY -> peer READY.
        private async ValueTask<ZMechanismResult?> ClientAsync(ZMechanismContext context, CancellationToken token)
        {
            await context.WriteCommandAsync(BuildHello(), token);

            var welcome = await context.ReadCommandAsync(token);
            if (welcome is null) return null;
            if (welcome.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(welcome.Value));
            if (!welcome.Value.Name.Span.SequenceEqual("WELCOME"u8))
                throw new ZMechanismException("expected WELCOME after HELLO");

            await context.WriteCommandAsync(context.LocalReadyBody, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(ready.Value));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after WELCOME");

            return new ZMechanismResult(context.Connection, ready.Value.Arguments.ToArray());
        }

        // Server (as-server = 1): HELLO -> authenticate -> WELCOME + local
        // READY -> peer READY. A rejected connection is answered with ERROR
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

            if (!authenticator!(user, peerPassword))
            {
                await context.WriteCommandAsync(ZmtpCommands.BuildError(RejectionReason), token);
                throw new ZMechanismException(RejectionReason);
            }

            await context.WriteCommandAsync(BuildWelcome(), token);
            await context.WriteCommandAsync(context.LocalReadyBody, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(PeerError(ready.Value));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after WELCOME");

            return new ZMechanismResult(context.Connection, ready.Value.Arguments.ToArray());
        }

        /// <summary>Extracts Username/Password from a HELLO body; both properties must be present.</summary>
        private static bool TryParseHello(ZMechanismCommand hello, out string username, out byte[] password)
        {
            var properties = ZmtpCommandCodec.ParseMetadata(hello.Arguments.Span);
            if (!properties.TryGetValue("Username", out var user) || !properties.TryGetValue("Password", out var pass))
            {
                username = string.Empty;
                password = [];
                return false;
            }

            username = user;
            password = Encoding.UTF8.GetBytes(pass);
            return true;
        }

        /// <summary>The peer's ERROR reason, or a protocol error if the ERROR body is malformed.</summary>
        private static string PeerError(ZMechanismCommand command)
        {
            return $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(command.Arguments.Span)}";
        }

        /// <summary>HELLO body: short-string name plus Username/Password metadata properties.</summary>
        private byte[] BuildHello()
        {
            var user = Encoding.UTF8.GetBytes(
                username is { } name
                    ? name
                    : throw new InvalidOperationException("a PLAIN client requires fixed credentials"));
            var metadataLength = ZmtpCommandCodec.MetadataPropertyLength("Username".Length, user.Length)
                                 + ZmtpCommandCodec.MetadataPropertyLength("Password".Length, password.Length);
            var body = new byte[1 + "HELLO".Length + metadataLength];
            body[0] = (byte)"HELLO".Length;
            "HELLO"u8.CopyTo(body.AsSpan(1));

            var offset = 1 + "HELLO".Length;
            offset += ZmtpCommandCodec.WriteMetadataProperty(body.AsSpan(offset), "Username"u8, user);
            ZmtpCommandCodec.WriteMetadataProperty(body.AsSpan(offset), "Password"u8, password.Span);
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
