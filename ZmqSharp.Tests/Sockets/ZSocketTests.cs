using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

public sealed class ZSocketTests
{
    [Fact]
    public async Task FairPolicy_RoundRobinsAcrossPeers()
    {
        await using var connA = CreateConnection();
        await using var connB = CreateConnection();
        var policy = new FairPolicy();
        var peers = new List<ZConnection> { connA, connB };
        using var message = ZMessage.FromOwned([1]);

        policy.RouteOutbound(message, peers).Should().ContainSingle().Which.Should().BeSameAs(connA);
        policy.RouteOutbound(message, peers).Should().ContainSingle().Which.Should().BeSameAs(connB);
        policy.RouteOutbound(message, peers).Should().ContainSingle().Which.Should().BeSameAs(connA);
    }

    [Fact]
    public async Task PairPolicy_RoutesToSinglePeer()
    {
        await using var connA = CreateConnection();
        var policy = new PairPolicy();
        var peers = new List<ZConnection> { connA };
        using var message = ZMessage.FromOwned([1]);

        policy.RouteOutbound(message, peers).Should().ContainSingle().Which.Should().BeSameAs(connA);
    }

    [Fact]
    public async Task PairSocket_RoundTripsMultipartOverTcp()
    {
        int port = GetFreePort();
        await using var server = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        await using var client = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        var serverMessages = server.Messages ?? throw new InvalidOperationException("receive channel not configured");
        var clientMessages = client.Messages ?? throw new InvalidOperationException("receive channel not configured");
        var echoTask = EchoAsync(server, serverMessages, cts.Token);
        byte[][] frames = ["ping"u8.ToArray(), "pong"u8.ToArray()];

        ZMessage? echo = null;
        for (int attempt = 0; attempt < 50 && echo is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart([..frames]), cts.Token);
            echo = await TryReadAsync(clientMessages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        var received = echo ?? throw new InvalidOperationException("no echo received within timeout");
        received.FrameCount.Should().Be(2);
        received.GetFrame(0).ToArray().Should().Equal(frames[0]);
        received.GetFrame(1).ToArray().Should().Equal(frames[1]);
        received.Dispose();

        cts.Cancel();
        try
        {
            await echoTask;
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Fact]
    public async Task DealerSocket_FairDispatch_ReachesBothPeers()
    {
        int portA = GetFreePort();
        int portB = GetFreePort();
        await using var serverA = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        await using var serverB = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        await using var dealer = ZSocket.Create(ZSocketType.Dealer, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await serverB.BindAsync($"tcp://127.0.0.1:{portB}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portB}", cts.Token);

        int countA = 0;
        int countB = 0;
        var drainA = DrainAsync(serverA, () => Interlocked.Increment(ref countA), cts.Token);
        var drainB = DrainAsync(serverB, () => Interlocked.Increment(ref countB), cts.Token);

        for (int attempt = 0; attempt < 100 && (countA < 1 || countB < 1); attempt++)
        {
            await dealer.SendAsync(ZMessage.FromOwned([1]), cts.Token);
            await Task.Delay(50, cts.Token);
        }

        countA.Should().BeGreaterThanOrEqualTo(1);
        countB.Should().BeGreaterThanOrEqualTo(1);

        cts.Cancel();
        try
        {
            await Task.WhenAll(drainA, drainB);
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Fact]
    public async Task SendAsync_DisposesMessageAfterRouting()
    {
        using var pool = new CountingMemoryPool();
        await using var socket = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { Pool = pool });
        var message = MessageFactory.PooledSingleFrame(pool, "hello"u8.ToArray());

        await socket.SendAsync(message);

        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task Close_CompletesReceiveChannel()
    {
        await using var socket = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 4 });
        await socket.CloseAsync();
        var messages = socket.Messages ?? throw new InvalidOperationException("receive channel not configured");
        messages.Completion.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task ProtocolError_CompletesChannelWithException()
    {
        int port = GetFreePort();
        await using var server = ZSocket.Create(ZSocketType.Pair, new ZSocketOptions { ReceiveChannelCapacity = 16 });
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        var badGreeting = new byte[64];
        await stream.WriteAsync(badGreeting);
        await stream.FlushAsync();

        var messages = server.Messages ?? throw new InvalidOperationException("receive channel not configured");
        await FluentActions.Awaiting(() => messages.Completion).Should().ThrowAsync<ZeroMqProtocolException>();
    }

    private static async Task EchoAsync(IZSocket server, ChannelReader<ZMessage> messages, CancellationToken token)
    {
        await foreach (var message in messages.ReadAllAsync(token))
        {
            await server.SendAsync(message, token);
        }
    }

    private static async Task DrainAsync(IZSocket socket, Action onMessage, CancellationToken token)
    {
        var messages = socket.Messages ?? throw new InvalidOperationException("receive channel not configured");
        await foreach (var message in messages.ReadAllAsync(token))
        {
            onMessage();
            message.Dispose();
        }
    }

    private static async Task<ZMessage?> TryReadAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan timeout,
        CancellationToken token)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(timeout);
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private static ZConnection CreateConnection()
        => new(
            new FakeTransport(new MemoryStream()),
            new ZReceiveOptions { Policy = ZReceiveMode.Borrowed },
            MemoryPool<byte>.Shared);

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class FakeTransport(Stream stream) : IZTransport
    {
        public Stream? Stream { get; } = stream;

        public ValueTask<IZTransport> AcceptAsync(CancellationToken token = default)
            => throw new NotSupportedException();

        public void Dispose() => Stream?.Dispose();
    }
}
