using System.Text;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security;

/// <summary>
/// The NULL mechanism (0016 section 5): no authentication. The session writes
/// the local READY immediately and reads the peer's READY - the exact wire
/// behavior of the previous hard-coded handshake, now behind the mechanism
/// boundary. Both roles are identical; the role only affects the greeting's
/// as-server bit, written by the handshake driver.
/// </summary>
public sealed class ZNullMechanism : IZSecurityMechanism
{
    /// <summary>Shared instance; the mechanism is stateless.</summary>
    public static ZNullMechanism Instance { get; } = new();

    public string Name => "NULL";

    public IZMechanismSession CreateSession(ZMechanismRole role)
    {
        return new NullSession();
    }

    private sealed class NullSession : IZMechanismSession
    {
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            // The driver already wrote the greeting; the NULL session sends
            // READY and waits for the peer's READY (or ERROR) - both sides
            // write READY immediately, exactly as before.
            await context.WriteCommandAsync(context.LocalReadyBody, token);

            ZMechanismCommand? command;
            while ((command = await context.ReadCommandAsync(token)) is not null)
            {
                if (command.Value.Name.Span.SequenceEqual("READY"u8))
                    return new ZMechanismResult(context.Connection, command.Value.Arguments.ToArray());

                if (command.Value.Name.Span.SequenceEqual("ERROR"u8))
                {
                    var reason = ZmtpCommandCodec.ParseErrorReason(command.Value.Arguments.Span);
                    throw new ZMechanismException($"peer sent ERROR: {reason}");
                }

                throw new ZMechanismException(
                    $"unknown command '{Encoding.ASCII.GetString(command.Value.Name.Span)}' during handshake");
            }

            return null;
        }
    }
}
