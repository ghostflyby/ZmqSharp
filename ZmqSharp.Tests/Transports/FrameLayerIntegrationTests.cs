using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// The frame-layer integration proofs (0033): each plug point hosts a real
/// socket end to end through the socket runtime. (a) A native frame transport
/// (an in-proc channel pair passing whole owned frames, zero-copy) via the
/// frame-transport factory and Bind/ConnectFrameAsync. (b) A custom framing
/// over a byte transport (a 4-byte big-endian length prefix instead of ZMTP's
/// flag+size) via the framing overload. (c) A custom wire protocol (a
/// greeting-less one-command establishment) via ZSocketOptions.WireProtocol.
/// </summary>
public sealed class FrameLayerIntegrationTests
{
    [Fact(Timeout = 15_000)]
    public async Task NativeFrameTransport_ExchangesMessagesEndToEnd()
    {
        await using var server = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        await using var client = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new LoopbackEndpoint();

        await server.BindFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);
        await client.ConnectFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);

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

        // Multipart frames keep their boundaries across the channel pair.
        await client.SendAsync(MessageFactory.Multipart([.. "one"u8], [.. "two"u8], [.. "three"u8]), token);
        using (var multipart = await server.Messages.ReadAsync(token))
        {
            Assert.Equal(3, multipart.Count);
            Assert.Equal([.. "one"u8], multipart[0].ToSequence().ToArray());
            Assert.Equal([.. "two"u8], multipart[1].ToSequence().ToArray());
            Assert.Equal([.. "three"u8], multipart[2].ToSequence().ToArray());
        }

        await client.DisconnectFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);
        await server.UnbindFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);
    }

    [Fact(Timeout = 15_000)]
    public async Task CustomFraming_OverASocketTransport_ExchangesMessages()
    {
        // A 4-byte length prefix per frame instead of ZMTP's flag+size: the
        // byte transport is the stock SocketTransport over loopback TCP, and
        // only the delimiting dialect changes.
        await using var server = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        await using var client = new ZPairSocket(
            new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, GetFreePort());

        await server.BindAsync<System.Net.IPEndPoint, SocketTransport>(endpoint, LengthPrefixFraming.Instance, token);
        await client.ConnectAsync<System.Net.IPEndPoint, SocketTransport>(endpoint, LengthPrefixFraming.Instance, token);

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
        // command frame in each direction, then frame-per-message traffic -
        // the shape a native frame transport wants when no raw greeting can
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

        await server.BindFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);
        await client.ConnectFrameAsync<LoopbackEndpoint, LoopbackFrameTransport>(endpoint, token);

        Assert.Equal(2, protocol.Establishments); // both endpoints ran the custom establishment

        await client.SendAsync(ZMessage.FromOwned([.. "protocol-frame"u8]), token);
        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal([.. "protocol-frame"u8], received[0].ToSequence().ToArray());
        }
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
    /// An in-proc frame transport: BindAsync registers the bound side in a
    /// static map; ConnectAsync pairs the two ends through a duplex channel
    /// of whole owned frames (the zero-copy native-frame path) and wakes the
    /// listener's accept loop.
    /// </summary>
    private sealed class LoopbackEndpoint;

    private sealed class LoopbackFrameTransport : IZFrameTransport<LoopbackFrameTransport, LoopbackEndpoint>
    {
        private static readonly Dictionary<LoopbackEndpoint, BoundPair> Bound = new();
        private static readonly Lock Gate = new();

        private BoundPair? _pair;
        private Func<IZFrameConnection, CancellationToken, ValueTask>? _onAccept;

        public event Func<IZFrameConnection, CancellationToken, ValueTask>? OnAccept
        {
            add => _onAccept += value;
            remove => _onAccept -= value;
        }

        public static async ValueTask<IZFrameConnection> ConnectAsync(LoopbackEndpoint endpoint,
            CancellationToken token = default)
        {
            BoundPair bound;
            lock (Gate)
            {
                if (!Bound.TryGetValue(endpoint, out bound!))
                    throw new InvalidOperationException("no loopback endpoint bound");
            }

            var connection = new LoopbackFrameConnection(bound.ServerToClient, bound.ClientToServer);
            bound.Incoming.SetResult(connection);
            return new LoopbackFrameConnection(bound.ClientToServer, bound.ServerToClient);
        }

        public static ValueTask<LoopbackFrameTransport> BindAsync(LoopbackEndpoint endpoint,
            CancellationToken token = default)
        {
            var transport = new LoopbackFrameTransport();
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
            public TaskCompletionSource<IZFrameConnection> Incoming { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);

            public Channel<ZWireFrame> ServerToClient { get; } =
                Channel.CreateUnbounded<ZWireFrame>(new UnboundedChannelOptions { SingleReader = true });

            public Channel<ZWireFrame> ClientToServer { get; } =
                Channel.CreateUnbounded<ZWireFrame>(new UnboundedChannelOptions { SingleReader = true });
        }
    }

    /// <summary>
    /// A channel-backed frame connection: reads take frame ownership from the
    /// channel; writes transfer the caller's frame into the channel. The
    /// peer's channel completion is EOF.
    /// </summary>
    private sealed class LoopbackFrameConnection(
        Channel<ZWireFrame> inbound, Channel<ZWireFrame> outbound) : IZFrameConnection
    {
        private bool _disposed;

        public async ValueTask<ZWireFrame?> ReadFrameAsync(CancellationToken token = default)
        {
            if (!await inbound.Reader.WaitToReadAsync(token)) return null;

            return await inbound.Reader.ReadAsync(token);
        }

        public ValueTask WriteFrameAsync(ZWireFrame frame, CancellationToken token = default)
            => outbound.Writer.WriteAsync(frame, token);

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

        public IZFrameConnection Adapt(IZConnection connection)
            => new LengthPrefixConnection(connection);

        private sealed class LengthPrefixConnection(IZConnection connection) : IZFrameConnection
        {
            private readonly byte[] _header = new byte[5];
            private readonly FrameSequence _wire = new();

            public async ValueTask<ZWireFrame?> ReadFrameAsync(CancellationToken token = default)
            {
                if (!await TryReadExactlyAsync(_header, token)) return null;

                var flags = (ZmtpFrameFlags)_header[0];
                ZWireFrame.Validate(flags);
                var length = BinaryPrimitives.ReadInt32BigEndian(_header.AsSpan(1));
                if (length < 0) throw new IOException("negative frame length");

                var owner = MemoryPool<byte>.Shared.Rent(length == 0 ? 1 : length);
                try
                {
                    if (!await TryReadExactlyAsync(owner.Memory[..length], token))
                    {
                        owner.Dispose();
                        return null;
                    }

                    var more = (flags & ZmtpFrameFlags.More) != 0;
                    return new ZWireFrame(flags, new ZFrame(new ZSegment(owner, 0, length), more));
                }
                catch
                {
                    owner.Dispose();
                    throw;
                }
            }

            public async ValueTask WriteFrameAsync(ZWireFrame frame, CancellationToken token = default)
            {
                ZWireFrame.Validate(frame.Flags);
                var body = frame.Frame.ToSequence();
                _header[0] = (byte)frame.Flags;
                BinaryPrimitives.WriteInt32BigEndian(_header.AsSpan(1), checked((int)body.Length));

                _wire.Clear();
                _wire.Add(_header.AsMemory());
                foreach (var segment in body) _wire.Add(segment);
                await connection.WriteAsync(_wire.Sequence, token);
            }

            public void Abort() => connection.Abort();

            public void Dispose() => connection.Dispose();

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
    /// A greeting-less wire protocol: establishment is one READY command
    /// frame in each direction (no greeting, no mechanism), then frame-per-
    /// message traffic straight through - the custom-protocol plug point.
    /// </summary>
    private sealed class HandshakeFreeProtocol : IZWireProtocol
    {
        public int Establishments { get; private set; }

        public IZWireEstablisher CreateEstablisher(
            IZFrameConnection frames, IZConnection? byteConnection,
            IZSecurityMechanism mechanism, ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
            MemoryPool<byte>? pool = null)
            => new Establisher(this, frames, localReadyBody);

        public IZWireSession CreateSession(IZFrameConnection frames, IZFrameCodec? codec = null,
            Action<Exception>? retire = null)
            => new Session(frames);

        private sealed class Establisher(HandshakeFreeProtocol owner, IZFrameConnection frames,
            ReadOnlyMemory<byte> localReadyBody) : IZWireEstablisher
        {
            public async ValueTask<ZMechanismResult?> EstablishAsync(CancellationToken token = default)
            {
                await WriteCommandAsync(frames, localReadyBody, token);
                if (await frames.ReadFrameAsync(token) is not { } peer) return null;

                byte[] peerReady;
                using (peer.Frame)
                {
                    if ((peer.Flags & ZmtpFrameFlags.Command) == 0)
                        throw new ZeroMqProtocolException("expected the peer's READY command frame");

                    // ZMechanismResult.PeerReadyBody is the metadata section
                    // after the command name - the same convention the
                    // mechanism context hands to protocol sessions.
                    var body = ZWireFrame.Contiguous(peer.Frame);
                    if (!ZmtpCommandCodec.TryReadCommandName(body.Span, out var name))
                        throw new ZeroMqProtocolException("malformed READY command name");

                    peerReady = body.Slice(1 + name.Length).ToArray();
                }

                owner.Establishments++;
                // The mechanism result carries the PEER's READY body (the
                // runtime parses the peer's socket type from it).
                return new ZMechanismResult(null, peerReady);
            }

            public void Dispose() { }
        }

        private sealed class Session(IZFrameConnection frames) : IZWireSession
        {
            private readonly SemaphoreSlim _gate = new(1, 1);
            private readonly FrameSequence _sequence = new();

            public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
            {
                await _gate.WaitAsync(token);
                try
                {
                    for (var i = 0; i < message.Count; i++)
                    {
                        var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                        await frames.WriteFrameAsync(new ZWireFrame(flags, message[i]), token);
                    }
                }
                finally
                {
                    _gate.Release();
                }
            }

            public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
                => WriteCommandAsync(frames, body, token);

            public IZWirePump CreatePump(
                Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
                ZFrameAllocator? allocator = null, long maxCommandSize = 0, long maxFrameLength = 0)
                => new Pump(frames, onFrame);

            public void Dispose() => _gate.Dispose();
        }

        private sealed class Pump(
            IZFrameConnection frames,
            Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame) : IZWirePump
        {
            public async ValueTask ParseAsync(CancellationToken token = default)
            {
                while (await frames.ReadFrameAsync(token) is { } wire)
                {
                    using var frame = wire.Frame;
                    if ((wire.Flags & ZmtpFrameFlags.Command) != 0) continue;

                    if (!await onFrame(this, wire.Frame, token)) await ResumeGate;
                }
            }

            private Task ResumeGate => Task.CompletedTask;

            public void Resume() { }
        }

        private static ValueTask WriteCommandAsync(IZFrameConnection frames,
            ReadOnlyMemory<byte> body, CancellationToken token)
            => frames.WriteFrameAsync(ZWireFrame.Copy(ZmtpFrameFlags.Command, body.Span).Wire, token);
    }
}
