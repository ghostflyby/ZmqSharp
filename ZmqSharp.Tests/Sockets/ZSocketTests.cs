using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
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

        ZMessage? echo = null;
        for (var attempt = 0; attempt < 50 && echo is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart([.. frames]), cts.Token);
            echo = await TryReadAsync(clientMessages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        var received = echo ?? throw new InvalidOperationException("no echo received within timeout");
        received.Count.Should().Be(2);
        received[0].ToSequence().ToArray().Should().Equal(frames[0]);
        received[1].ToSequence().ToArray().Should().Equal(frames[1]);
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
        await using var serverA = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var serverB = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
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
        await using var serverA = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
        await using var serverB = ZSocket.CreateDealer(new ZQueueSocketOptions { ReceiveCapacity = 16 });
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

                var payload = message.Value[0].ToSequence().ToArray();
                if (payload.AsSpan().SequenceEqual("a"u8))
                {
                    hasA = true;
                }
                else if (payload.AsSpan().SequenceEqual("b"u8))
                {
                    hasB = true;
                }

                message.Value.Dispose();
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

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value[0].TryGetValue(out ZSegment segment).Should().BeTrue();
        segment.GetOwnedArray(out var array).Should().BeTrue();
        array.Should().Equal("hello"u8.ToArray());
        var outstandingBeforeDispose = pool.Outstanding;
        received.Value.Dispose();
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

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart("a"u8.ToArray(), "b"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value.Count.Should().Be(2);
        received.Value[0].TryGetValue(out ZSegment first);
        first.GetOwnedArray(out _).Should().BeFalse();
        received.Value[1].TryGetValue(out ZSegment second);
        second.GetOwnedArray(out _).Should().BeTrue();
        received.Value.Dispose();
    }

    [Fact]
    public async Task SegmentedMaterialization_SplitsLargeFrameIntoSegments()
    {
        var payload = new byte[9000];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i % 251);
        }

        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            ReceivePolicy = new ZReceiveOptions { ContiguousFrameLimit = 100 },
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned(payload), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value[0].TryGetValue(out ZSegments _).Should().BeTrue();
        received.Value[0].ToSequence().ToArray().Should().Equal(payload);
        received.Value.Dispose();
    }

    [Fact]
    public async Task SegmentedMaterialization_MultipartFramesStayIndependent()
    {
        var first = new byte[9000];
        var second = new byte[9000];
        for (var i = 0; i < first.Length; i++)
        {
            first[i] = (byte)(i % 251);
            second[i] = (byte)((i + 7) % 251);
        }

        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            ReceivePolicy = new ZReceiveOptions { ContiguousFrameLimit = 100 },
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart(first, second), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value.Count.Should().Be(2);
        received.Value[0].TryGetValue(out ZSegments _).Should().BeTrue();
        received.Value[1].TryGetValue(out ZSegments _).Should().BeTrue();
        received.Value[0].ToSequence().ToArray().Should().Equal(first);
        received.Value[1].ToSequence().ToArray().Should().Equal(second);
        received.Value.Dispose();
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
        var received = new List<ZMessage>();
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

                received.Add(message.Value);
            }
        }

        var smallMessage = received.First(message => message[0].ToSequence().Length == small.Length);
        var largeMessage = received.First(message => message[0].ToSequence().Length == large.Length);
        smallMessage[0].TryGetValue(out ZSegment smallSegment);
        smallSegment.GetOwnedArray(out _).Should().BeFalse();
        largeMessage[0].TryGetValue(out ZSegment largeSegment);
        largeSegment.GetOwnedArray(out _).Should().BeTrue();
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

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned("x"u8.ToArray()), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value[0].TryGetValue(out ZSegment segment);
        segment.GetOwnedArray(out _).Should().BeTrue();
        var outstandingBeforeDispose = pool.Outstanding;
        received.Value.Dispose();
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
            frame.TryGetValue(out ZSegment segment);
            current.Add(segment.Memory.ToArray());
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
                    message.Value.Dispose();
                }
            }

            received.Should().BeTrue();
            await cts.CancelAsync();
        }
    }

    [Fact]
    public async Task ConnectAsync_PeerClosesDuringHandshake_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        raw.Dispose();

        Exception? caught = null;
        try
        {
            await connectTask;
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        caught.Should().NotBeNull();
        (caught is IOException or SocketException).Should().BeTrue();
    }

    [Fact]
    public async Task ConnectAsync_PeerSendsMalformedGreeting_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(new byte[64]);
        await stream.FlushAsync();

        Func<Task> act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_PeerSocketTypeMismatch_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("DEALER")));
        await stream.FlushAsync();

        Func<Task> act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();

        // RFC 23: the peer must receive an ERROR command before the disconnect.
        var received = new MemoryStream();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            var chunk = new byte[256];
            while (true)
            {
                var count = await stream.ReadAsync(chunk, timeout.Token);
                if (count == 0)
                {
                    break;
                }

                received.Write(chunk, 0, count);
                if (received.ToArray().AsSpan().IndexOf("ERROR"u8) >= 0)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }

        received.ToArray().AsSpan().IndexOf("ERROR"u8).Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public async Task ConnectAsync_PeerSocketTypeMatch_Completes()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR")));
        await stream.FlushAsync();

        await connectTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ConnectAsync_CancellationDuringHandshake_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();
        using var cts = new CancellationTokenSource();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        Func<Task> act = () => connectTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ConnectAsync_DisconnectDuringHandshake_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreatePair();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();

        // Wait for the client's handshake bytes so the peer is registered before
        // disconnecting; otherwise DisconnectAsync races connection registration.
        await stream.ReadExactlyAsync(new byte[64]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await client.DisconnectAsync<EndPoint, SocketTransport>(new IPEndPoint(IPAddress.Loopback, port));

        Exception? caught = null;
        try
        {
            await connectTask.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        caught.Should().NotBeNull();
        caught.Should().NotBeOfType<TimeoutException>();
        (caught is OperationCanceledException or IOException or SocketException or ObjectDisposedException).Should().BeTrue();
    }

    [Fact]
    public async Task ConnectAsync_DealerPeerRep_Completes()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreateDealer();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("REP")));
        await stream.FlushAsync();

        await connectTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ConnectAsync_DealerPeerReq_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = ZSocket.CreateDealer();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("REQ")));
        await stream.FlushAsync();

        Func<Task> act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_SynchronousHandshakeFailure_LeavesNoRoutablePeer()
    {
        await using var client = ZSocket.CreatePairCallback();

        Func<Task> act = () => client.ConnectAsync<EndPoint, SynchronousEofTransport>(
            new IPEndPoint(IPAddress.Loopback, 1));
        await act.Should().ThrowAsync<IOException>();

        // A failed attempt must not leave a dead peer routable: sending with no
        // established peers drops the message instead of faulting the socket.
        await client.SendAsync("x"u8.ToArray());
    }

    [Fact]
    public async Task CallbackException_StillRaisesPeerEndedAndReclaimsPool()
    {
        using var pool = new CountingMemoryPool();
        var server = ZSocket.CreatePairCallback(new ZSocketOptions { Pool = pool });
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");
        server.OnFrame += (_, _) => throw new InvalidOperationException("callback failed");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"), ZmtpTestData.Frame("boom"u8.ToArray())));
        await stream.FlushAsync();

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        failure.Should().BeOfType<InvalidOperationException>();
        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task OversizedFrame_RejectsPeerAndReclaimsPool()
    {
        using var pool = new CountingMemoryPool();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            MaxFrameLength = 4,
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        rejected.Rejection.Limit.Should().Be(4);
        rejected.Rejection.Actual.Should().Be(5);
        server.ReceiveRejections.Should().Be(1);
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task OversizedMessage_RejectsBeforeDeliveringAnyFrame()
    {
        using var pool = new CountingMemoryPool();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            MaxMessageLength = 10,
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(MessageFactory.Multipart("aaaaaa"u8.ToArray(), "bbbbbb"u8.ToArray()), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);
        rejected.Rejection.Limit.Should().Be(10);
        rejected.Rejection.Actual.Should().Be(12);
        server.ReceiveRejections.Should().Be(1);
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task RejectedPeer_IsNotRoutable_NoFurtherMessages()
    {
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            MaxFrameLength = 4,
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // A message below the limit establishes and flows first.
        var first = false;
        for (var attempt = 0; attempt < 50 && !first; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned("ok"u8.ToArray()), cts.Token);
            var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
            if (message is not null)
            {
                message.Value.Dispose();
                first = true;
            }
        }

        first.Should().BeTrue();

        // The oversized message rejects the peer; later sends must not be
        // delivered to the server.
        await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);
        await client.SendAsync(ZMessage.FromOwned("ok"u8.ToArray()), cts.Token);

        var extra = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(500), cts.Token);
        extra.Should().BeNull();
    }

    [Fact]
    public async Task RejectedPeer_EndsWithClose_NotWithPeerError()
    {
        var clientEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            MaxFrameLength = 4,
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        client.PeerEnded += (_, failure) => clientEnded.TrySetResult(failure);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);

        // Traffic-phase rejection is a plain close, never an ERROR command
        // (0008 D5): the client sees EOF/IO, not a peer protocol error.
        var failure = await clientEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        (failure is null or IOException or SocketException).Should().BeTrue();
    }

    [Fact]
    public async Task OverLimitFrame_DoesNotRentFromPool()
    {
        using var pool = new ProbingMemoryPool();
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            Pool = pool,
            MaxFrameLength = 4,
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // Prove the probe works for in-limit frames.
        await client.SendAsync(ZMessage.FromOwned("ok"u8.ToArray()), cts.Token);
        var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(500), cts.Token);
        message.Should().NotBeNull();
        message.Value.Dispose();
        pool.Rentals.Should().BeGreaterThan(0);

        // The over-limit frame is rejected before any allocation.
        pool.Reset();
        await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);
        await Task.Delay(200, cts.Token);
        pool.Rentals.Should().Be(0);
    }

    [Fact]
    public async Task Limits_EnforcedOutsidePolicy_CustomPolicyCannotBypass()
    {
        // The policy is allocation-only and cannot reject or bypass the
        // socket-level limits: they are enforced by the connection guard
        // before Decide is called.
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveCapacity = 4,
            MaxFrameLength = 4,
            ReceivePolicy = new ZDelegateReceivePolicy(
                _ => new ZReceiveAllocation { Mode = ZReceiveMode.Pooled }),
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions { ReceiveCapacity = 4 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(ZMessage.FromOwned("hello"u8.ToArray()), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        server.ReceiveRejections.Should().Be(1);
    }

    private static async Task EchoAsync(ZQueueSocket<ZPairSocket> server, ChannelReader<ZMessage> messages, CancellationToken token)
    {
        await foreach (var message in messages.ReadAllAsync(token))
        {
            await server.SendAsync(message, token);
        }
    }

    private static async Task DrainAsync<TSocket>(ZQueueSocket<TSocket> socket, Action onMessage, CancellationToken token)
        where TSocket : ZSocketBase
    {
        var messages = socket.Messages;
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

/// <summary>Transport whose connection immediately reports EOF so the pump completes synchronously.</summary>
internal sealed class SynchronousEofTransport : IZTransport<SynchronousEofTransport, EndPoint>
{
    public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
    {
        add { }
        remove { }
    }

    public static ValueTask<IZConnection> ConnectAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
        => ValueTask.FromResult<IZConnection>(new SynchronousEofConnection());

    public static ValueTask<SynchronousEofTransport> BindAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
        => ValueTask.FromResult(new SynchronousEofTransport());

    public ValueTask StartAsync(CancellationToken token = default) => ValueTask.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class SynchronousEofConnection : IZConnection
{
    private int disposed;

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        => ValueTask.FromResult(0);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        return ValueTask.CompletedTask;
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
        => WriteAsync(frame, token);

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => WriteAsync(body, token);

    public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        => WriteAsync(ReadOnlyMemory<byte>.Empty, token);

    public bool OnFrame(ZFrame frame, CancellationToken token) => true;

    public void SetFrameHandler(Func<ZFrame, CancellationToken, bool> onFrame)
    {
    }

    public void SetConnectionEndedHandler(Action onConnectionEnded)
    {
    }

    public void OnConnectionEnded()
    {
    }

    public void Dispose() => Interlocked.Exchange(ref disposed, 1);
}
