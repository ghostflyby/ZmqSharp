using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZMTP establishment path (0016 section 4): writes the local greeting,
/// reads and validates the peer greeting, matches the configured mechanism by
/// name, runs the mechanism's handshake session, and yields the session
/// connection plus the peer READY metadata for the socket layer. Runs once per
/// connection; the mechanism session drives the command sequence between the
/// greeting and the established state. A mechanism mismatch sends an ERROR
/// command before faulting (the RFC 23 pattern the socket-type check also
/// uses).
/// </summary>
internal sealed class ZmtpHandshake : IDisposable
{
    private readonly IZConnection connection;
    private readonly IZSecurityMechanism mechanism;
    private readonly ReadOnlyMemory<byte> localReadyBody;
    private readonly int maxCommandSize;
    private readonly MemoryPool<byte> pool;

    internal ZmtpHandshake(
        IZConnection connection,
        IZSecurityMechanism mechanism,
        ReadOnlyMemory<byte> localReadyBody,
        int maxCommandSize,
        MemoryPool<byte>? pool = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(mechanism);
        this.connection = connection;
        this.mechanism = mechanism;
        this.localReadyBody = localReadyBody;
        this.maxCommandSize = maxCommandSize;
        this.pool = pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>
    /// Completes the greeting exchange and the mechanism handshake. Returns
    /// null when the peer closed during establishment.
    /// </summary>
    public async ValueTask<ZMechanismResult?> EstablishAsync(ZMechanismRole role, CancellationToken token = default)
    {
        // Greeting and commands are separate gated writes; the write gate
        // serializes them and nothing else writes before establishment, so
        // no coalescing is needed (0016 D5).
        await connection.WriteAsync(ZmtpGreeting.Build(mechanism.Name, role), token);

        var peerMechanism = await ReadPeerMechanismAsync(token);
        if (peerMechanism is null) return null;

        if (!string.Equals(peerMechanism, mechanism.Name, StringComparison.Ordinal))
        {
            await connection.SendCommandAsync(ZmtpCommands.BuildError("Invalid mechanism"), token);
            throw new ZeroMqProtocolException(
                $"peer mechanism '{peerMechanism}' does not match the configured mechanism '{mechanism.Name}'");
        }

        using var context = new ZMechanismContext(connection, localReadyBody, maxCommandSize, pool);
        var result = await mechanism.CreateSession(role).RunAsync(context, token);
        if (result is not null && result.Value.SessionConnection is null)
            throw new InvalidOperationException("mechanism session returned a null session connection");

        return result;
    }

    public void Dispose()
    {
        // The mechanism session's context is disposed when RunAsync returns;
        // the raw connection ownership stays with the socket pump.
    }

    private async ValueTask<string?> ReadPeerMechanismAsync(CancellationToken token)
    {
        // The greeting is a fixed 64-byte, once-per-connection read on the
        // cold establishment path, so it uses a plain array instead of a pool
        // rent: the socket pool is then rented exactly once during the
        // handshake (by the mechanism context), keeping the measured receive
        // path's per-connection baseline stable (AllocationTests).
        var greeting = new byte[64];
        if (!await TryReadExactlyAsync(greeting, token)) return null;

        return ZmtpGreeting.ParseMechanism(greeting);
    }

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await connection.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }
}
