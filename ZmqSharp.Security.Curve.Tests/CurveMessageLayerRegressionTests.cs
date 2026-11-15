using System.Buffers;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Security.Curve.Tests;

/// <summary>
/// Codec-path regressions from the 0033 review: the assembled codec staging
/// over native message connections, bounded command staging, and sink
/// retention across reads with the pooled fallback decoded allocator.
/// </summary>
public sealed class CurveMessageLayerRegressionTests
{
    private static (Key32 ServerSecret, Key32 ServerPublic, Key32 ClientSecret, Key32 ClientPublic) Keys()
    {
        CurveCrypto.GenerateKeyPair(out var serverPublic, out var serverSecret);
        CurveCrypto.GenerateKeyPair(out var clientPublic, out var clientSecret);
        return (serverSecret, serverPublic, clientSecret, clientPublic);
    }

    [Fact(Timeout = 60_000)]
    public async Task NativeMessageTransport_MultipartStaysDistinctUnderCurve()
    {
        // The assembled codec staging once staged every frame of a multipart
        // message over ONE reused buffer, aliasing all frames into the last
        // frame's bytes; only a native (non-framing) message connection
        // reaches that path. CURVE both directions.
        var keys = Keys();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(keys.ServerSecret.Span) },
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new CurveMechanism(keys.ClientSecret.Span, keys.ServerPublic.Span),
            },
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
        });
        var token = TestContext.Current.CancellationToken;
        var endpoint = new CurveLoopbackEndpoint();

        await server.BindMessageAsync<CurveLoopbackEndpoint, CurveLoopbackTransport>(endpoint, token);
        await client.ConnectMessageAsync<CurveLoopbackEndpoint, CurveLoopbackTransport>(endpoint, token);

        byte[][] parts =
        [
            "alpha-alpha-alpha"u8.ToArray(), "beta-beta-beta-beta"u8.ToArray(), "gamma"u8.ToArray(),
        ];
        await client.SendAsync(ZMessage.Copy(parts), token);
        using (var received = await server.Messages.ReadAsync(token))
        {
            Assert.Equal(3, received.Count);
            for (var i = 0; i < 3; i++) Assert.Equal(parts[i], received[i].ToSequence().ToArray());
        }

        await server.SendAsync(ZMessage.Copy(parts), token);
        using (var echoed = await client.Messages.ReadAsync(token))
        {
            Assert.Equal(3, echoed.Count);
            for (var i = 0; i < 3; i++) Assert.Equal(parts[i], echoed[i].ToSequence().ToArray());
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task CommandFlood_ThenDataMessage_ReadsCorrectly()
    {
        // A codec session once grew its command staging without bound; the
        // release after each consumed command keeps a flood bounded, and the
        // data messages interleaved with it still read back in order.
        using var recording = new RecordingByteConnection();
        using var codec = NewCodec();
        const int rounds = 200;
        using (var session = ZmqProtocol.Instance.CreateSession(
                   ZmtpFraming.Instance.Adapt(recording), codec))
        {
            var message = ZMessage.FromOwned([.. "data"u8]);
            var token = TestContext.Current.CancellationToken;
            for (var i = 0; i < rounds; i++)
            {
                await session.SendAsync(message, token);
                await session.SendCommandAsync(new byte[] { 4, (byte)'P', (byte)'I', (byte)'N', (byte)'G' }, token);
            }
        }

        using var input = new RecordingByteConnection(recording.Recorded);
        using var readCodec = NewCodec();
        using var readSession = ZmqProtocol.Instance.CreateSession(
            ZmtpFraming.Instance.Adapt(input), readCodec);
        var seen = 0;
        while (await readSession.ReadMessageAsync(TestContext.Current.CancellationToken) is { } message)
        {
            using (message)
            {
                seen++;
                Assert.Equal([.. "data"u8], message[0].ToSequence().ToArray());
            }
        }

        Assert.Equal(rounds, seen);
    }

    [Fact(Timeout = 15_000)]
    public async Task MessageSink_RetainedMessagesStayIntactAcrossReads()
    {
        // A codec socket on the sink surface gets the pooled fallback decoded
        // allocator, so a sink holding messages past further reads (its
        // contract allows arbitrary retention) sees intact frames.
        var keys = Keys();
        var retained = new List<ZMessage>();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var postSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(keys.ServerSecret.Span) },
            MessageSink = new CollectingSink(m =>
            {
                retained.Add(m);
                if (retained.Count == 3) received.TrySetResult();
                if (retained.Count == 4) postSeen.TrySetResult();
            }),
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new CurveMechanism(keys.ClientSecret.Span, keys.ServerPublic.Span),
            },
        });
        var token = TestContext.Current.CancellationToken;
        var endpoint = $"tcp://127.0.0.1:{GetFreePort()}";
        await server.BindAsync(endpoint, token);
        await client.ConnectAsync(endpoint, token);

        for (var i = 0; i < 3; i++)
            await client.SendAsync(ZMessage.FromOwned([(byte)('a' + i), (byte)i]), token);
        await received.Task.WaitAsync(token);

        // Further traffic flows while the sink still holds earlier messages;
        // the retained frames must not see later decode buffers.
        await client.SendAsync(ZMessage.FromOwned([.. "post"u8]), token);
        await postSeen.Task.WaitAsync(token);
        Assert.Equal(4, retained.Count);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal([(byte)('a' + i), (byte)i], retained[i][0].ToSequence().ToArray());
            retained[i].Dispose();
        }
    }

    private static CurveFrameCodec NewCodec() => new(Key32.From(new byte[32]),
        encodeServerToClient: false, decodeServerToClient: false, 1, 0);

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class CollectingSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}


internal sealed class RecordingByteConnection(byte[]? feed = null, int capacity = 0, bool yieldWrites = false)
    : IZConnection
{
    private readonly byte[] _feed = feed ?? [];
    private readonly List<byte> _written = new(capacity);
    private int _position;
    public byte[] Recorded => [.. _written];

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var length = Math.Min(destination.Length, _feed.Length - _position);
        _feed.AsMemory(_position, length).CopyTo(destination);
        _position += length;
        return ValueTask.FromResult(length);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        if (yieldWrites) await Task.Yield();
        token.ThrowIfCancellationRequested();
        _written.AddRange(bytes.Span);
    }

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        foreach (var segment in bytes) await WriteAsync(segment, token);
    }

    public void Abort() { }
    public void Dispose() { }
}


/// <summary>An in-proc endpoint identity for the curve regressions.</summary>
internal sealed class CurveLoopbackEndpoint;

/// <summary>
/// A minimal retaining in-proc message transport (the zero-copy handoff the
/// retention contract covers), mirroring the integration suite's loopback.
/// </summary>
internal sealed class CurveLoopbackTransport
    : IZMessageTransport<CurveLoopbackTransport, CurveLoopbackEndpoint>
{
    private static readonly Dictionary<CurveLoopbackEndpoint, BoundPair> Bound = new();
    private static readonly Lock Gate = new();

    private BoundPair? _pair;
    private Func<IZMessageConnection, CancellationToken, ValueTask>? _onAccept;

    public event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept
    {
        add => _onAccept += value;
        remove => _onAccept -= value;
    }

    public static async ValueTask<IZMessageConnection> ConnectAsync(CurveLoopbackEndpoint endpoint,
        CancellationToken token = default)
    {
        BoundPair bound;
        lock (Gate)
        {
            if (!Bound.TryGetValue(endpoint, out bound!))
                throw new InvalidOperationException("no loopback endpoint bound");
        }

        var connection = new CurveLoopbackConnection(bound.ServerToClient, bound.ClientToServer);
        bound.Incoming.SetResult(connection);
        return new CurveLoopbackConnection(bound.ClientToServer, bound.ServerToClient);
    }

    public static ValueTask<CurveLoopbackTransport> BindAsync(CurveLoopbackEndpoint endpoint,
        CancellationToken token = default)
    {
        var transport = new CurveLoopbackTransport();
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

internal sealed class CurveLoopbackConnection(
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

    public bool RetainsMessages => true;

    public void Abort() => outbound.Writer.TryComplete();

    public void Dispose()
    {
        if (_disposed) return;

        _disposed = true;
        outbound.Writer.TryComplete();
    }
}
