using System.Text;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Extensibility;

/// <summary>
/// A PLAIN (RFC 27) mechanism built EXCLUSIVELY from ZmqSharp's public API.
/// This is the extensibility gate of 0016: the in-library PLAIN slice must not
/// need anything this type cannot use. The identical source compiled in a
/// separate probe project WITHOUT InternalsVisibleTo (so no ZmqSharp internal
/// was reachable) - that is what proves the seam; the tests here verify the
/// mechanism actually completes and rejects handshakes over real sockets.
/// </summary>
public sealed class PlainMechanism : IZSecurityMechanism
{
    private readonly string? username;
    private readonly ReadOnlyMemory<byte> password;
    private readonly Func<string, ReadOnlyMemory<byte>, bool>? authenticator;

    /// <summary>Client role: fixed credentials sent in HELLO.</summary>
    public PlainMechanism(string username, ReadOnlySpan<byte> password)
    {
        ArgumentNullException.ThrowIfNull(username);
        this.username = username;
        this.password = password.ToArray();
    }

    /// <summary>Server role: authenticates HELLO credentials per connection.</summary>
    public PlainMechanism(Func<string, ReadOnlyMemory<byte>, bool> authenticator)
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
        Func<string, ReadOnlyMemory<byte>, bool>? authenticator) : IZMechanismSession
    {
        public ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            return role == ZMechanismRole.Client
                ? ClientAsync(context, token)
                : ServerAsync(context, token);
        }

        // Client: HELLO -> WELCOME -> READY -> peer READY
        private async ValueTask<ZMechanismResult?> ClientAsync(ZMechanismContext context, CancellationToken token)
        {
            await context.WriteCommandAsync(BuildHello(), token);

            var welcome = await context.ReadCommandAsync(token);
            if (welcome is null) return null;
            if (welcome.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(ZmtpCommandCodec.ParseErrorReason(welcome.Value.Arguments.Span));
            if (!welcome.Value.Name.Span.SequenceEqual("WELCOME"u8))
                throw new ZMechanismException("expected WELCOME after HELLO");

            await context.WriteCommandAsync(context.LocalReadyBody, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(ZmtpCommandCodec.ParseErrorReason(ready.Value.Arguments.Span));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after WELCOME");

            return new ZMechanismResult(context.Connection, ready.Value.Arguments.ToArray());
        }

        // Server: HELLO -> (authenticate) -> WELCOME + READY -> peer READY
        private async ValueTask<ZMechanismResult?> ServerAsync(ZMechanismContext context, CancellationToken token)
        {
            var hello = await context.ReadCommandAsync(token);
            if (hello is null) return null;
            if (hello.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(ZmtpCommandCodec.ParseErrorReason(hello.Value.Arguments.Span));
            if (!hello.Value.Name.Span.SequenceEqual("HELLO"u8))
                throw new ZMechanismException("expected HELLO");

            var properties = ZmtpCommandCodec.ParseMetadata(hello.Value.Arguments.Span);
            if (!properties.TryGetValue("Username", out var user)
                || !properties.TryGetValue("Password", out var peerPassword))
            {
                await context.WriteCommandAsync(ZmtpCommands.BuildError("Invalid username or password"), token);
                throw new ZMechanismException("HELLO is missing Username or Password");
            }

            // The authenticator is public-surface; credentials arrive as the
            // UTF-8 text decoded by the shared metadata parser.
            var accepted = authenticator!(user, Encoding.UTF8.GetBytes(peerPassword));
            if (!accepted)
            {
                await context.WriteCommandAsync(ZmtpCommands.BuildError("Invalid username or password"), token);
                throw new ZMechanismException("Invalid username or password");
            }

            await context.WriteCommandAsync(BuildWelcome(), token);
            await context.WriteCommandAsync(context.LocalReadyBody, token);

            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (ready.Value.Name.Span.SequenceEqual("ERROR"u8))
                throw new ZMechanismException(ZmtpCommandCodec.ParseErrorReason(ready.Value.Arguments.Span));
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY after WELCOME");

            return new ZMechanismResult(context.Connection, ready.Value.Arguments.ToArray());
        }

        /// <summary>HELLO body: short-string name plus Username/Password metadata properties.</summary>
        private byte[] BuildHello()
        {
            var user = Encoding.UTF8.GetBytes(username!);
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
