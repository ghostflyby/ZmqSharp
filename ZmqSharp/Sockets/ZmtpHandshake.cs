using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// The ZMTP establishment path (0016 section 4): writes the local greeting,
/// reads and validates the peer greeting, matches the configured mechanism by
/// name, runs the mechanism's handshake session, and yields the session
/// optional frame codec plus the peer READY metadata for the socket layer. Runs once per
/// connection; the mechanism session drives the command sequence between the
/// greeting and the established state. A mechanism mismatch sends an ERROR
/// command before faulting (the RFC 23 pattern the socket-type check also
/// uses).
/// </summary>
internal sealed class ZmtpHandshake : IDisposable
{
    private readonly IZConnection _connection;
    private readonly IZSecurityMechanism _mechanism;
    private readonly ReadOnlyMemory<byte> _localReadyBody;
    private readonly long _maxCommandSize;
    private readonly MemoryPool<byte> _pool;

    internal ZmtpHandshake(
        IZConnection connection,
        IZSecurityMechanism mechanism,
        ReadOnlyMemory<byte> localReadyBody,
        long maxCommandSize,
        MemoryPool<byte>? pool = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(mechanism);
        _connection = connection;
        _mechanism = mechanism;
        _localReadyBody = localReadyBody;
        _maxCommandSize = maxCommandSize;
        _pool = pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>
    /// Completes the greeting exchange and the mechanism handshake. Returns
    /// null when the peer closed during establishment.
    /// </summary>
    public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
    {
        // Traffic waits on establishment, so this handshake is the only writer.
        await _connection.WriteAsync(ZmtpGreeting.Build(_mechanism.Name, _mechanism.Role), token);

        var peerMechanism = await ReadPeerMechanismAsync(token);
        if (peerMechanism is null) return null;

        if (!string.Equals(peerMechanism, _mechanism.Name, StringComparison.Ordinal))
        {
            await new ZmtpFrameEncoder(_connection).WriteCommandAsync(ZmtpCommands.BuildError("Invalid mechanism"), token);
            throw new ZeroMqProtocolException(
                $"peer mechanism '{peerMechanism}' does not match the configured mechanism '{_mechanism.Name}'");
        }

        using var context = new ZMechanismContext(_connection, _localReadyBody, _maxCommandSize, _pool);
        var result = await _mechanism.CreateSession().RunAsync(context, token);
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
            var count = await _connection.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }
}
