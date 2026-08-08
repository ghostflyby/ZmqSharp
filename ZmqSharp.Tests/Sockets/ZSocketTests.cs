using System.Buffers;
using System.Collections.Concurrent;
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
        var hasA = false;
        var hasB = false;
        for (var attempt = 0; attempt < 50 && (!hasA || !hasB); attempt++)
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

                if (message[0].ToArray().AsSpan().SequenceEqual("a"u8))
                {
                    hasA = true;
                }
                else if (message[0].ToArray().AsSpan().SequenceEqual("b"u8))
                {
                    hasB = true;
                }

                message.Dispose();
            }
        }

        hasA.Should().BeTrue();
        hasB.Should().BeTrue();
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
    public async Task ReceivePolicy_DecideOwned_NeverTouchesPool()
    {
        using var pool = new CountingMemoryPool();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            ReceivePolicy = new ZDelegateReceivePolicy(
                _ => new ZReceiveAllocation { Mode = ZReceiveMode.Owned }),
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        IZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received!.TryGetOwnedArray(0, out var array).Should().BeTrue();
        array.Should().Equal("hello"u8.ToArray());
        var outstandingBeforeDispose = pool.Outstanding;
        received.Dispose();
        pool.Outstanding.Should().Be(outstandingBeforeDispose);
    }

    [Fact]
    public async Task ReceivePolicy_DecidePerFrame_SplitsModesWithinMessage()
    {
        using var pool = new CountingMemoryPool();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            ReceivePolicy = new ZDelegateReceivePolicy(
                ctx => ctx.FrameIndex == 0
                    ? new ZReceiveAllocation { Mode = ZReceiveMode.Pooled }
                    : new ZReceiveAllocation { Mode = ZReceiveMode.Owned }),
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        IZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Count.Should().Be(2);
        received.TryGetOwnedArray(0, out _).Should().BeFalse();
        received.TryGetOwnedArray(1, out _).Should().BeTrue();
        received.Dispose();
    }

    [Fact]
    public void ReceiveOptions_Decide_UsesContiguousFrameLimit()
    {
        var policy = new ZReceiveOptions { ContiguousFrameLimit = 100 };

        var small = policy.Decide(new ZReceiveContext { FrameLength = 10 });
        small.Mode.Should().Be(ZReceiveMode.Pooled);
        small.Segmented.Should().BeFalse();

        var large = policy.Decide(new ZReceiveContext { FrameLength = 200 });
        large.Mode.Should().Be(ZReceiveMode.Pooled);
        large.Segmented.Should().BeTrue();
    }

    [Fact]
    public async Task ReceivePolicy_DecideByFrameLength_MixesModes()
    {
        using var pool = new CountingMemoryPool();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 8,
            Pool = pool,
            ReceivePolicy = new ZDelegateReceivePolicy(
                ctx => ctx.FrameLength > 100
                    ? new ZReceiveAllocation { Mode = ZReceiveMode.Owned }
                    : new ZReceiveAllocation { Mode = ZReceiveMode.Pooled }),
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 8 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        var small = new byte[10];
        var large = new byte[200];
        var received = new List<IZMessage>();
        for (var attempt = 0; attempt < 50 && received.Count < 2; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned(small), cts.Token);
            await client.SendAsync(ZMessage.FromOwned(large), cts.Token);

            for (var i = 0; i < 2; i++)
            {
                var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
                if (message is null)
                {
                    break;
                }

                received.Add(message);
            }
        }

        var smallMessage = received.First(message => message[0].Length == small.Length);
        var largeMessage = received.First(message => message[0].Length == large.Length);
        smallMessage.TryGetOwnedArray(0, out _).Should().BeFalse();
        largeMessage.TryGetOwnedArray(0, out _).Should().BeTrue();
        smallMessage.Dispose();
        largeMessage.Dispose();
    }

    [Fact]
    public async Task ReceivePolicy_DefaultModeOwned_NeverTouchesPool()
    {
        using var pool = new CountingMemoryPool();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            ReceivePolicy = new ZReceiveOptions
            {
                Mode = ZReceiveMode.Owned
            },
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        IZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned("x"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.TryGetOwnedArray(0, out _).Should().BeTrue();
        var outstandingBeforeDispose = pool.Outstanding;
        received.Dispose();
        pool.Outstanding.Should().Be(outstandingBeforeDispose);
    }

    [Fact]
    public async Task Close_CompletesReceiveChannel()
    {
        var socket = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        await socket.DisposeAsync();
        socket.Messages.Completion.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task ProtocolError_EndsPeerWithoutCompletingChannel()
    {
        var port = GetFreePort();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        var badGreeting = new byte[64];
        await stream.WriteAsync(badGreeting);
        await stream.FlushAsync();

        var messages = server.Messages;
        (await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5))).Should().BeOfType<ZeroMqProtocolException>();
        messages.Completion.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task ConcurrentSends_DoNotInterleaveMultipartFrames()
    {
        var port = GetFreePort();
        await using var server = ZSocket.CreatePairCallback(new ZSocketOptions());
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 64 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        var received = new ConcurrentQueue<byte[][]>();
        var current = new List<byte[]>();
        server.OnFrame += (frame, ct) =>
        {
            current.Add(frame.Memory.ToArray());
            if (!frame.More)
            {
                received.Enqueue([.. current]);
                current.Clear();
            }

            return true;
        };

        var senderA = SendLoopAsync(client, 0x61, cts.Token);
        var senderB = SendLoopAsync(client, 0x62, cts.Token);
        await Task.WhenAll(senderA, senderB);

        received.Should().NotBeEmpty();
        foreach (var message in received)
        {
            message.Should().HaveCount(3);
            message[0][0].Should().Be(message[1][0]);
            message[0][0].Should().Be(message[2][0]);
        }
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

        // The reset may surface as an IO error (Windows/macOS), a clean EOF
        // (Linux), or be dropped before the connection is set up (macOS);
        // in every case the socket must dispose without faulting.
        var ended = await Task.WhenAny(peerEnded.Task, Task.Delay(TimeSpan.FromSeconds(1)));
        if (ended == peerEnded.Task)
        {
            (await peerEnded.Task is null or IOException).Should().BeTrue();
        }

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

    private static async Task SendLoopAsync(ZQueueSocket<ZPairSocket> client, byte tag, CancellationToken token)
    {
        var frame = new byte[64];
        frame[0] = tag;
        for (var i = 0; i < 50; i++)
        {
            using var message = MessageFactory.Multipart(frame, frame, frame);
            await client.SendAsync(message, token);
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
