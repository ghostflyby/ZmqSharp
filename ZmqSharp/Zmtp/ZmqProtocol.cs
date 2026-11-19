using System.Buffers;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The built-in ZMTP 3.0 protocol over the message layer (0033): the 64-byte
/// greeting exchange (raw on byte-origin connections - wire-identical to
/// every ZMTP peer - and one command on native message connections), the
/// configured security mechanism's command exchange, and message-per-unit
/// traffic through the optional codec. This is the protocol tcp/ipc have
/// always spoken, decomposed so its framing lives in
/// <see cref="ZmtpFraming"/> and its semantics here.
/// </summary>
public sealed class ZmqProtocol : IZWireProtocol
{
    public static ZmqProtocol Instance { get; } = new();

    private ZmqProtocol()
    {
    }

    public async ValueTask<ZMechanismResult?> EstablishAsync(
        IZMessageConnection connection,
        IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
        CancellationToken token = default)
    {
        // The 64-byte greeting is a stream construct: raw on the byte origin
        // (wire-identical to every ZMTP peer), one command on native message
        // connections.
        string? peerMechanism;
        if (connection.ByteOrigin is { } bytes)
        {
            await bytes.WriteAsync(ZmtpGreeting.Build(mechanism.Name, mechanism.Role), token);

            var greeting = new byte[64];
            var filled = 0;
            while (filled < greeting.Length)
            {
                var count = await bytes.ReadAsync(greeting.AsMemory(filled), token);
                if (count == 0) return null;

                filled += count;
            }

            peerMechanism = ZmtpGreeting.ParseMechanism(greeting);
        }
        else
        {
            await connection.WriteCommandAsync(ZmtpGreeting.Build(mechanism.Name, mechanism.Role), token);

            if (await connection.ReadUnitAsync(token) is not { } reply) return null;

            using (reply.Message)
            {
                if (!reply.IsCommand || ZWireFrameLogic.ByteLength(reply.Message[0]) != 64)
                    throw new ZeroMqProtocolException("expected the greeting as the first command");

                peerMechanism = ZmtpGreeting.ParseMechanism(
                    ZWireFrameLogic.Contiguous(reply.Message[0]).Span);
            }
        }

        if (!string.Equals(peerMechanism, mechanism.Name, StringComparison.Ordinal))
        {
            await connection.WriteCommandAsync(ZmtpCommands.BuildError("Invalid mechanism"), token);
            throw new ZeroMqProtocolException(
                $"peer mechanism '{peerMechanism}' does not match the configured mechanism '{mechanism.Name}'");
        }

        using var context = new ZMechanismContext(
            new UnitCommandChannel(connection, maxCommandSize), localReadyBody, maxCommandSize);
        return await mechanism.CreateSession().RunAsync(context, token);
    }

    public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
        Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
        long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue,
        MemoryPool<byte>? pool = null)
        => new ZMessageSession(connection, codec, retire, decodedAllocator, maxCommandSize, maxFrameLength,
            pool ?? MemoryPool<byte>.Shared);
}
