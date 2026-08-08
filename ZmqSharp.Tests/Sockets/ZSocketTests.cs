using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

public sealed class ZSocketTests
{
    [Fact]
    public async Task PairSocket_RoundTripsMultipartOverTcp()
    {
        var port = GetFreePort();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        var serverMessages = server.Messages;
        var clientMessages = client.Messages;
        var echoTask = EchoAsync(server, serverMessages, cts.Token);
        byte[][] frames = ["ping"u8.ToArray(), "pong"u8.ToArray()];

        IZMessage? echo = null;
        for (var attempt = 0; attempt < 50 && echo is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart([..frames]), cts.Token);
            echo = await TryReadAsync(clientMessages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        var received = echo ?? throw new InvalidOperationException("no echo received within timeout");
        received.Count.Should().Be(2);
        received[0].ToArray().Should().Equal(frames[0]);
        received[1].ToArray().Should().Equal(frames[1]);
        received.Dispose();

        await cts.CancelAsync();
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
        var portA = GetFreePort();
        var portB = GetFreePort();
        await using var serverA = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var serverB = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var dealer = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await serverB.BindAsync($"tcp://127.0.0.1:{portB}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portB}", cts.Token);

        var countA = 0;
        var countB = 0;
        var drainA = DrainAsync(serverA, () => Interlocked.Increment(ref countA), cts.Token);
        var drainB = DrainAsync(serverB, () => Interlocked.Increment(ref countB), cts.Token);

        for (var attempt = 0; attempt < 100 && (countA < 1 || countB < 1); attempt++)
        {
            await dealer.SendAsync(ZMessage.FromOwned([1]), cts.Token);
            await Task.Delay(50, cts.Token);
        }

        countA.Should().BeGreaterThanOrEqualTo(1);
        countB.Should().BeGreaterThanOrEqualTo(1);

        await cts.CancelAsync();
        try
        {
            await Task.WhenAll(drainA, drainB);
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Fact]
    public async Task DealerSocket_ReceivesFromBothPeers()
    {
        var portA = GetFreePort();
        var portB = GetFreePort();
        await using var serverA = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var serverB = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var dealer = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await serverB.BindAsync($"tcp://127.0.0.1:{portB}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await dealer.ConnectAsync($"tcp://127.0.0.1:{portB}", cts.Token);

        var messages = dealer.Messages;
        var received = new List<byte[]>();
        for (var attempt = 0; attempt < 50 && received.Count < 2; attempt++)
        {
            await serverA.SendAsync(ZMessage.FromOwned("a"u8.ToArray()), cts.Token);
            await serverB.SendAsync(ZMessage.FromOwned("b"u8.ToArray()), cts.Token);

            for (var i = 0; i < 2; i++)
            {
                var message = await TryReadAsync(messages, TimeSpan.FromMilliseconds(200), cts.Token);
                if (message is null)
                {
                    break;
                }

                received.Add(message[0].ToArray());
                message.Dispose();
            }
        }

        received.Any(frame => frame.AsSpan().SequenceEqual("a"u8)).Should().BeTrue();
        received.Any(frame => frame.AsSpan().SequenceEqual("b"u8)).Should().BeTrue();
    }

    [Fact]
    public async Task SendAsync_DisposesMessageAfterRouting()
    {
        using var pool = new CountingMemoryPool();
        await using var socket = ZSocket.CreatePairCallback(new ZSocketOptions { Pool = pool });
        var message = MessageFactory.PooledSingleFrame(pool, "hello"u8.ToArray());

        await socket.SendAsync(message);

        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task Close_CompletesReceiveChannel()
    {
        await using var socket = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        await socket.DisposeAsync();
        socket.Messages.Completion.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task ProtocolError_CompletesChannelWithException()
    {
        var port = GetFreePort();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        var badGreeting = new byte[64];
        await stream.WriteAsync(badGreeting);
        await stream.FlushAsync();

        var messages = server.Messages;
        await FluentActions.Awaiting(() => messages.Completion).Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task PeerConnectionReset_DoesNotFaultDispose()
    {
        var port = GetFreePort();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePairCallback(new ZSocketOptions());
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready()));
        await stream.FlushAsync();

        // Abortive close: force a reset to end the peer abruptly.
        raw.Client.LingerState = new LingerOption(true, 0);
        raw.Dispose();

        // The reset surfaces as an IO error on Windows/macOS and as clean EOF
        // on Linux; either way the peer ends without faulting the socket.
        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        (failure is null or IOException).Should().BeTrue();

        await server.DisposeAsync();
    }

    [Fact]
    public async Task SendRacingHandshake_DoesNotCorruptPeerHandshake()
    {
        for (var attempt = 0; attempt < 40; attempt++)
        {
            var port = GetFreePort();
            await using var server = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
            await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
            await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

            // A send before the peer is routable is dropped; resend until the
            // message flows, which also proves the handshake was not corrupted.
            var received = false;
            for (var retry = 0; retry < 20 && !received; retry++)
            {
                await server.SendAsync(ZMessage.FromOwned("x"u8.ToArray()), cts.Token);
                var message = await TryReadAsync(client.Messages, TimeSpan.FromMilliseconds(20), cts.Token);
                if (message is not null)
                {
                    received = true;
                    message.Dispose();
                }
            }

            received.Should().BeTrue();
            await cts.CancelAsync();
        }
    }

    private static async Task EchoAsync(ZQueueSocket<ZPairSocket> server, ChannelReader<IZMessage> messages, CancellationToken token)
    {
        await foreach (var message in messages.ReadAllAsync(token))
        {
            await server.SendAsync(message, token);
        }
    }

    private static async Task DrainAsync(ZQueueSocket<ZPairSocket> socket, Action onMessage, CancellationToken token)
    {
        var messages = socket.Messages;
        await foreach (var message in messages.ReadAllAsync(token))
        {
            onMessage();
            message.Dispose();
        }
    }

    private static async Task<IZMessage?> TryReadAsync(
        ChannelReader<IZMessage> reader,
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

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

}
