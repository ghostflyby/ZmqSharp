using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// The message-layer integration proofs (0033, connection model in 0034):
/// each plug point hosts a real socket end to end through the socket runtime.
/// (a) A native message transport (an in-proc channel pair passing whole
/// messages, zero-copy) through the one transport factory and
/// Bind/ConnectAsync. (b) A custom framing over a byte transport (a 4-byte
/// big-endian length prefix instead of ZMTP's flag+size) via
/// ZSocketOptions.Framing. (c) A custom wire protocol (a greeting-less
/// one-command establishment) via ZSocketOptions.WireProtocol.
/// </summary>
public sealed class FrameLayerIntegrationTests
{
    [Fact(Timeout = 15_000)]
    public async Task NativeMessageTransport_ExchangesMessagesEndToEnd()
    {
        await using var server = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        await using var client = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new LoopbackEndpoint();

        await server.BindAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);
        await client.ConnectAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);

        await client.SendAsync(ZMessage.FromOwned([.. "alpha"u8]), token);
        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "alpha"u8], received[0].ToSequence().ToArray());
        }

        await server.SendAsync(ZMessage.FromOwned([.. "beta"u8]), token);
        using (var echoed = await client.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "beta"u8], echoed[0].ToSequence().ToArray());
        }

        // Multipart messages cross the channel pair whole.
        await client.SendAsync(MessageFactory.Multipart([.. "one"u8], [.. "two"u8], [.. "three"u8]), token);
        using (var multipart = await server.Messages.ReadAsync(token))
        {
            Assert.Equal(3, multipart.Count);
            Assert.Equal([.. "one"u8], multipart[0].ToSequence().ToArray());
            Assert.Equal([.. "two"u8], multipart[1].ToSequence().ToArray());
            Assert.Equal([.. "three"u8], multipart[2].ToSequence().ToArray());
        }

        await client.DisconnectAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);
        await server.UnbindAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);
    }

    [Fact(Timeout = 15_000)]
    public async Task CustomFraming_OverASocketTransport_ExchangesMessages()
    {
        // A 4-byte length prefix per frame instead of ZMTP's flag+size: the
        // byte transport is the stock SocketTransport over loopback TCP, and
        // only the delimiting dialect changes.
        var options = new ZSocketOptions
        {
            Framing = LengthPrefixFraming.Instance,
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
        };
        await using var server = new ZPairSocket(options);
        await using var client = new ZPairSocket(options);
        var token = TestContext.Current.CancellationToken;
        var endpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, GetFreePort());

        await server.BindAsync<System.Net.IPEndPoint, SocketTransport>(endpoint, token);
        await client.ConnectAsync<System.Net.IPEndPoint, SocketTransport>(endpoint, token);

        await client.SendAsync(ZMessage.FromOwned([.. "over-custom-framing"u8]), token);
        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "over-custom-framing"u8], received[0].ToSequence().ToArray());
        }

        await server.SendAsync(ZMessage.FromOwned([.. "reply"u8]), token);
        using (var echoed = await client.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "reply"u8], echoed[0].ToSequence().ToArray());
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task CustomWireProtocol_EstablishesWithoutGreeting_AndExchangesFrames()
    {
        // A greeting-less protocol: the establishment is a single READY
        // command in each direction, then message-per-unit traffic - the
        // shape a native message transport wants when no raw greeting can
        // exist. Both ends run the custom protocol via WireProtocol.
        var protocol = new HandshakeFreeProtocol();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            WireProtocol = protocol,
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            WireProtocol = protocol,
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
        });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new LoopbackEndpoint();

        await server.BindAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);
        await client.ConnectAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);

        // ConnectAsync waits only for the client's own establishment;
        // the accept side runs the same protocol concurrently, so wait for
        // both ends instead of sampling the counter (0033 CI flake).
        await protocol.BothEstablished.WaitAsync(token);
        Assert.Equal(2, protocol.Establishments); // both endpoints ran the custom establishment

        await client.SendAsync(ZMessage.FromOwned([.. "protocol-frame"u8]), token);
        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "protocol-frame"u8], received[0].ToSequence().ToArray());
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task RetainingTransport_PooledMessageContentSurvivesSend()
    {
        // The retention contract (0033): a retaining connection takes
        // ownership at completion, so the runtime must NOT dispose a pooled
        // message under the receiving chain - the peer reads intact frames,
        // and its own dispose returns the buffers exactly once.
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions { Pool = pool });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new LoopbackEndpoint();

        await server.BindAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);
        await client.ConnectAsync<LoopbackEndpoint, LoopbackMessageTransport>(endpoint, token);

        var payload = new byte[2048];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 251);
        var pooledOwner = pool.Rent(payload.Length);
        payload.CopyTo(pooledOwner.Memory);
        await client.SendAsync(new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(pooledOwner, 0, payload.Length)))), token);

        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal(payload, received[0].ToSequence().ToArray());
        }

        // The receiving chain disposed the message; the buffers returned to
        // the shared pool (both sockets rent from it).
        await pool.WaitForOutstandingAsync(2, TimeSpan.FromSeconds(5));
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// An in-proc message transport: BindAsync registers the bound side in a
    /// static map; ConnectAsync pairs the two ends through a duplex channel
    /// of whole wire units (the zero-copy native-message path) and wakes the
    /// listener's accept loop.
    /// </summary>
    private sealed class LoopbackEndpoint;

    private sealed class LoopbackMessageTransport : IZTransport<LoopbackMessageTransport, LoopbackEndpoint>
    {
        private static readonly Dictionary<LoopbackEndpoint, BoundPair> Bound = new();
        private static readonly Lock Gate = new();

        private BoundPair? _pair;
        private Func<IZMessageConnection, CancellationToken, ValueTask>? _onAccept;

        public event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept
        {
            add => _onAccept += value;
            remove => _onAccept -= value;
        }

        public static async ValueTask<IZMessageConnection> ConnectAsync(LoopbackEndpoint endpoint,
            ZTransportContext context, CancellationToken token = default)
        {
            BoundPair bound;
            lock (Gate)
            {
                if (!Bound.TryGetValue(endpoint, out bound!))
                    throw new InvalidOperationException("no loopback endpoint bound");
            }

            var connection = new LoopbackMessageConnection(bound.ServerToClient, bound.ClientToServer);
            bound.Incoming.SetResult(connection);
            return new LoopbackMessageConnection(bound.ClientToServer, bound.ServerToClient);
        }

        public static ValueTask<LoopbackMessageTransport> BindAsync(LoopbackEndpoint endpoint,
            ZTransportContext context, CancellationToken token = default)
        {
            var transport = new LoopbackMessageTransport();
            lock (Gate)
            {
                transport._pair = new BoundPair();
                Bound[endpoint] = transport._pair;
            }

            return ValueTask.FromResult(transport);
        }

        public async ValueTask StartAsync(CancellationToken token = default)
        {
            if (_pair is not { } pair) throw new InvalidOperationException("not bound");

            // The accept loop parks until the first connect pairs the ends
            // (single-peer PAIR semantics; the registration stays live).
            var connection = await pair.Incoming.Task.WaitAsync(token);
            if (_onAccept is { } handler) await handler(connection, token);
        }

        public void Dispose()
        {
            lock (Gate)
            {
                foreach (var (endpoint, pair) in Bound.ToList())
                    if (ReferenceEquals(pair, _pair))
                        Bound.Remove(endpoint);
            }
        }

        private sealed class BoundPair
        {
            public TaskCompletionSource<IZMessageConnection> Incoming { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);

            public Channel<ZWireUnit> ServerToClient { get; } =
                Channel.CreateUnbounded<ZWireUnit>(new UnboundedChannelOptions { SingleReader = true });

            public Channel<ZWireUnit> ClientToServer { get; } =
                Channel.CreateUnbounded<ZWireUnit>(new UnboundedChannelOptions { SingleReader = true });
        }
    }

    /// <summary>
    /// A channel-backed message connection: reads take unit ownership from
    /// the channel; writes transfer the caller's message into the channel.
    /// The peer's channel completion is EOF.
    /// </summary>
    private sealed class LoopbackMessageConnection(
        Channel<ZWireUnit> inbound, Channel<ZWireUnit> outbound) : IZMessageConnection
    {
        private bool _disposed;

        public async ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
        {
            if (!await inbound.Reader.WaitToReadAsync(token)) return null;

            return await inbound.Reader.ReadAsync(token);
        }

        public ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
            => outbound.Writer.WriteAsync(new ZWireUnit(ZmtpFrameFlags.None, message), token);

        public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
            => outbound.Writer.WriteAsync(
                new ZWireUnit(ZmtpFrameFlags.Command, ZMessage.FromOwned(body.ToArray())), token);

        /// <summary>
        /// Zero-copy channel handoff: the unit stays referenced until the
        /// receiving chain disposes it (0033's retention contract).
        /// </summary>
        public bool RetainsMessages => true;

        public void Abort() => outbound.Writer.TryComplete();

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            outbound.Writer.TryComplete();
        }
    }

    /// <summary>A 4-byte big-endian length prefix per frame; flags ride a leading byte.</summary>
    private sealed class LengthPrefixFraming : IZFraming
    {
        public static LengthPrefixFraming Instance { get; } = new();

        public IZMessageConnection Adapt(
            IZConnection connection, in ZTransportContext context, bool ownsConnection = true)
            => new LengthPrefixConnection(connection, context.Pool, ownsConnection);

        private sealed class LengthPrefixConnection(
            IZConnection connection, MemoryPool<byte> pool, bool ownsConnection) : IZMessageConnection
        {
            private readonly byte[] _header = new byte[5];
            private readonly FrameSequence _wire = new();
            private readonly List<ZFrame> _frames = [];

            public async ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
            {
                while (true)
                {
                    if (!await TryReadExactlyAsync(_header, token)) return null;

                    var flags = (ZmtpFrameFlags)_header[0];
                    ZWireFrameLogic.Validate(flags);
                    var length = BinaryPrimitives.ReadInt32BigEndian(_header.AsSpan(1));
                    if (length < 0) throw new IOException("negative frame length");

                    var more = (flags & ZmtpFrameFlags.More) != 0;
                    var owner = pool.Rent(length == 0 ? 1 : length);
                    ZFrame frame;
                    try
                    {
                        if (!await TryReadExactlyAsync(owner.Memory[..length], token))
                        {
                            owner.Dispose();
                            return null;
                        }

                        frame = new ZFrame(new ZSegment(owner, 0, length), more);
                    }
                    catch
                    {
                        owner.Dispose();
                        throw;
                    }

                    if ((flags & ZmtpFrameFlags.Command) != 0)
                        return new ZWireUnit(ZmtpFrameFlags.Command, new ZMessage(new ZSingleMessage(frame)));

                    _frames.Add(frame);
                    if (more) continue;

                    var message = _frames.Count == 1
                        ? new ZMessage(new ZSingleMessage(_frames[0]))
                        : new ZMessage(new ZMultiMessage([.. _frames]));
                    _frames.Clear();
                    return new ZWireUnit(ZmtpFrameFlags.None, message);
                }
            }

            public async ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
            {
                for (var i = 0; i < message.Count; i++)
                    await WriteFrameAsync(i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None,
                        message[i], token);
            }

            public async ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
            {
                var pooled = ZWireFrameLogic.Copy(ZmtpFrameFlags.Command, body.Span);
                try
                {
                    await WriteFrameAsync(ZmtpFrameFlags.Command, pooled.Wire, token);
                }
                finally
                {
                    pooled.Owner.Dispose();
                }
            }

            public bool RetainsMessages => false;

            public void Abort()
            {
                if (ownsConnection) connection.Abort();
            }

            public void Dispose()
            {
                if (ownsConnection) connection.Dispose();
            }

            private async ValueTask WriteFrameAsync(ZmtpFrameFlags flags, ZFrame frame, CancellationToken token)
            {
                ZWireFrameLogic.Validate(flags);
                var body = frame.ToSequence();
                _header[0] = (byte)flags;
                BinaryPrimitives.WriteInt32BigEndian(_header.AsSpan(1), checked((int)body.Length));

                _wire.Clear();
                _wire.Add(_header.AsMemory());
                foreach (var segment in body) _wire.Add(segment);
                await connection.WriteAsync(_wire.Sequence, token);
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
    }

    /// <summary>
    /// A greeting-less wire protocol: establishment is one READY command in
    /// each direction (no greeting, no mechanism), then message-per-unit
    /// traffic straight through - the custom-protocol plug point.
    /// </summary>
    private sealed class HandshakeFreeProtocol : IZWireProtocol
    {
        private readonly TaskCompletionSource _bothEstablished =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _establishments;

        public int Establishments => Volatile.Read(ref _establishments);

        /// <summary>Completes once both endpoints have run their establishment.</summary>
        public Task BothEstablished => _bothEstablished.Task;

        public IZWireEstablisher CreateEstablisher(
            IZMessageConnection connection,
            IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
            MemoryPool<byte>? pool = null)
            => new Establisher(this, connection, localReadyBody);

        public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec = null,
            Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
            long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue)
            => new Session(connection);

        private sealed class Establisher(HandshakeFreeProtocol owner, IZMessageConnection connection,
            ReadOnlyMemory<byte> localReadyBody) : IZWireEstablisher
        {
            public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
            {
                await connection.WriteCommandAsync(localReadyBody, token);
                if (await connection.ReadUnitAsync(token) is not { } peer) return null;

                byte[] peerReady;
                using (peer.Message)
                {
                    if (!peer.IsCommand || peer.Message.Count != 1)
                        throw new ZeroMqProtocolException("expected the peer's READY command");

                    // ZMechanismResult.PeerReadyBody is the metadata section
                    // after the command name - the same convention the
                    // mechanism context hands to protocol sessions.
                    var body = ZWireFrameLogic.Contiguous(peer.Message[0]);
                    if (!ZmtpCommandCodec.TryReadCommandName(body.Span, out var name))
                        throw new ZeroMqProtocolException("malformed READY command name");

                    peerReady = body.Slice(1 + name.Length).ToArray();
                }

                // Both sides establish concurrently: the increment must be
                // atomic, and the count reaching two wakes the test.
                if (Interlocked.Increment(ref owner._establishments) == 2)
                    owner._bothEstablished.TrySetResult();

                return new ZMechanismResult(null, peerReady);
            }

            public void Dispose() { }
        }

        private sealed class Session(IZMessageConnection connection) : IZWireSession
        {
            private readonly SemaphoreSlim _gate = new(1, 1);

            public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
            {
                await _gate.WaitAsync(token);
                try
                {
                    await connection.WriteMessageAsync(message, token);
                }
                finally
                {
                    _gate.Release();
                }
            }

            public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
                => connection.WriteCommandAsync(body, token);

            public async ValueTask<ZMessage?> ReadMessageAsync(CancellationToken token = default)
            {
                while (await connection.ReadUnitAsync(token) is { } unit)
                {
                    if (unit.IsCommand)
                    {
                        unit.Message.Dispose();
                        continue;
                    }

                    return unit.Message;
                }

                return null;
            }

            public bool RetainsSentMessages => connection.RetainsMessages;

            public void Dispose() => _gate.Dispose();
        }
    }
}
