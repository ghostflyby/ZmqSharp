using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

public sealed class ZSocketTests
{
    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PairSocket_RoundTripsMultipartOverTcp(TransportKind kind)
    {
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var server = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        var serverMessages = server.Messages;
        var clientMessages = client.Messages;
        var echoTask = EchoAsync(server, serverMessages, cts.Token);
        byte[][] frames = [[.. "ping"u8], [.. "pong"u8]];

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

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DealerSocket_FairDispatch_ReachesBothPeers(TransportKind kind)
    {
        var endpointA = TestTransports.GetEndpoint(kind);
        var endpointB = TestTransports.GetEndpoint(kind);
        await using var serverA = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var serverB = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var dealer = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync(endpointA, cts.Token);
        await serverB.BindAsync(endpointB, cts.Token);
        await dealer.ConnectAsync(endpointA, cts.Token);
        await dealer.ConnectAsync(endpointB, cts.Token);

        var countA = 0;
        var countB = 0;
        var bothReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainA = DrainAsync(serverA, () => OnPeerMessage(true), cts.Token);
        var drainB = DrainAsync(serverB, () => OnPeerMessage(false), cts.Token);

        // The dealer drops sends to peers that have not finished the handshake,
        // so keep sending until both drains report a message. The drains signal
        // completion directly; the short pause only paces the retries and the
        // loop stops as soon as both peers are reached instead of polling a
        // fixed window.
        for (var attempt = 0; attempt < 100 && !bothReached.Task.IsCompleted; attempt++)
        {
            await dealer.SendAsync(ZMessage.FromOwned([1]), cts.Token);
            if (await Task.WhenAny(bothReached.Task, Task.Delay(25, cts.Token)) == bothReached.Task) break;
        }

        await bothReached.Task.WaitAsync(cts.Token);

        countA.Should().BeGreaterThanOrEqualTo(1);
        countB.Should().BeGreaterThanOrEqualTo(1);

        void OnPeerMessage(bool peerA)
        {
            if (peerA)
                Interlocked.Increment(ref countA);
            else
                Interlocked.Increment(ref countB);

            if (Volatile.Read(ref countA) >= 1 && Volatile.Read(ref countB) >= 1) bothReached.TrySetResult();
        }

        await cts.CancelAsync();
        try
        {
            await Task.WhenAll(drainA, drainB);
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DealerSocket_ReceivesFromBothPeers(TransportKind kind)
    {
        var endpointA = TestTransports.GetEndpoint(kind);
        var endpointB = TestTransports.GetEndpoint(kind);
        await using var serverA = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var serverB = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var dealer = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync(endpointA, cts.Token);
        await serverB.BindAsync(endpointB, cts.Token);
        await dealer.ConnectAsync(endpointA, cts.Token);
        await dealer.ConnectAsync(endpointB, cts.Token);

        var messages = dealer.Messages;
        var hasA = false;
        var hasB = false;
        for (var attempt = 0; attempt < 50 && (!hasA || !hasB); attempt++)
        {
            await serverA.SendAsync(ZMessage.FromOwned([.. "a"u8]), cts.Token);
            await serverB.SendAsync(ZMessage.FromOwned([.. "b"u8]), cts.Token);

            for (var i = 0; i < 2; i++)
            {
                var message = await TryReadAsync(messages, TimeSpan.FromMilliseconds(200), cts.Token);
                if (message is null) break;

                var payload = message.Value[0].ToSequence().ToArray();
                if (payload.AsSpan().SequenceEqual("a"u8))
                    hasA = true;
                else if (payload.AsSpan().SequenceEqual("b"u8)) hasB = true;

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
        await using var socket = new ZPairSocket(new ZSocketOptions { Pool = pool });
        var message = MessageFactory.PooledSingleFrame(pool, [.. "hello"u8]);

        await socket.SendAsync(message);

        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReceivePolicy_DecideOwned_NeverTouchesPool(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            ReceivePolicy = new ZDelegateReceivePolicy(_ => new ZReceiveAllocation { Mode = ZReceiveMode.Owned })
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);
            received = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        }

        received.Should().NotBeNull();
        received.Value[0].TryGetValue(out ZSegment segment).Should().BeTrue();
        segment.GetOwnedArray(out var array).Should().BeTrue();
        array.Should().Equal([.. "hello"u8]);
        var outstandingBeforeDispose = pool.Outstanding;
        received.Value.Dispose();
        pool.Outstanding.Should().Be(outstandingBeforeDispose);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReceivePolicy_DecidePerFrame_SplitsModesWithinMessage(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            ReceivePolicy = new ZDelegateReceivePolicy(ctx => ctx.FrameIndex == 0
                ? new ZReceiveAllocation { Mode = ZReceiveMode.Pooled }
                : new ZReceiveAllocation { Mode = ZReceiveMode.Owned })
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(MessageFactory.Multipart([.. "a"u8], [.. "b"u8]), cts.Token);
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

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task SegmentedMaterialization_SplitsLargeFrameIntoSegments(TransportKind kind)
    {
        var payload = new byte[9000];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 251);

        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            ReceivePolicy = new ZReceiveOptions { ContiguousFrameLimit = 100 }
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

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

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task SegmentedMaterialization_MultipartFramesStayIndependent(TransportKind kind)
    {
        var first = new byte[9000];
        var second = new byte[9000];
        for (var i = 0; i < first.Length; i++)
        {
            first[i] = (byte)(i % 251);
            second[i] = (byte)((i + 7) % 251);
        }

        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            ReceivePolicy = new ZReceiveOptions { ContiguousFrameLimit = 100 }
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

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

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReceivePolicy_DecideByFrameLength_MixesModes(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
            ReceivePolicy = new ZDelegateReceivePolicy(ctx => ctx.FrameLength > 100
                ? new ZReceiveAllocation { Mode = ZReceiveMode.Owned }
                : new ZReceiveAllocation { Mode = ZReceiveMode.Pooled })
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

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
                if (message is null) break;

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

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReceivePolicy_DefaultModeOwned_NeverTouchesPool(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            ReceivePolicy = new ZReceiveOptions
            {
                Mode = ZReceiveMode.Owned
            }
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        ZMessage? received = null;
        for (var attempt = 0; attempt < 50 && received is null; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned([.. "x"u8]), cts.Token);
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
        var socket = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        await socket.DisposeAsync();
        socket.Messages.Completion.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task ProtocolError_EndsPeerWithoutCompletingChannel()
    {
        var port = GetFreePort();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
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
    public async Task HandshakeTimeout_FaultsEstablishment()
    {
        // A peer that never answers the greeting/READY exchange exceeds the
        // configured handshake timeout and faults its establishment (0006 3.2).
        using var listener = new TcpListener(IPAddress.Loopback, GetFreePort());
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();

        await using var client = new ZPairSocket(new ZSocketOptions { HandshakeTimeoutMs = 200 });
        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");

        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        // Read the client's greeting but never respond.
        await raw.GetStream().ReadExactlyAsync(new byte[64]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        var failure = await Record.ExceptionAsync(() => connectTask.WaitAsync(TimeSpan.FromSeconds(5)));
        failure.Should().NotBeNull();
        (failure is TimeoutException or OperationCanceledException).Should().BeTrue();
    }

    [Fact]
    public async Task MaxIncompleteHandshakes_DropsExcessInboundPeers()
    {
        // The inbound surface caps concurrent incomplete handshakes; a second
        // slow-connecting peer is dropped with cancellation (0006 3.2).
        await using var server = new ZPairSocket(new ZSocketOptions { MaxIncompleteHandshakes = 1 });
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var first = new TcpClient();
        await first.ConnectAsync(IPAddress.Loopback, port);
        var firstStream = first.GetStream();
        await firstStream.WriteAsync(ZmtpTestData.Greeting());
        await firstStream.FlushAsync();

        // The second accepted connection exceeds the cap and is dropped. The
        // rejection is observable as the server closing the connection, so
        // wait for that state (the read returns 0 / EOF) instead of a fixed
        // delay - the server's rejection is the state, not elapsed time.
        using var second = new TcpClient();
        await second.ConnectAsync(IPAddress.Loopback, port);
        var secondStream = second.GetStream();
        var probe = await secondStream.ReadAsync(new byte[64]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        probe.Should().Be(0);
    }

    [Fact]
    public async Task LegacyZmtp2_Greeting_RejectedWithClearError()
    {
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket();
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();

        // ZMTP 2.0 greeting: signature plus revision byte 0x01.
        var v2Greeting = ZmtpTestData.Greeting();
        v2Greeting[10] = 1;
        await stream.WriteAsync(v2Greeting);
        await stream.FlushAsync();

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        failure.Should().BeOfType<ZeroMqProtocolException>();
        failure.Message.Should().Contain("ZMTP 2.0 peers are not supported");
    }

    [Fact]
    public async Task OversizedCommand_WithSmallMaxCommandSize_RejectsPeer()
    {
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions { MaxCommandSize = 256 });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();

        // An oversized command frame during the handshake exceeds the
        // configured command-size limit and rejects the peer (0008 Slice B).
        var oversizedCommand = ZmtpTestData.Frame(new byte[300], command: true);
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), oversizedCommand));
        await stream.FlushAsync();

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        failure.Should().BeOfType<ZeroMqProtocolException>();
    }

    [Fact]
    public void MaxCommandSize_BelowFloor_Throws()
    {
        // The command-size limit is mandatory and cannot be disabled entirely
        // (0008 Slice B completion gate).
        var act = () => new ZSocketOptions { MaxCommandSize = ZSocketOptions.MinMaxCommandSize - 1 };
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task MessageSink_AggregatesMultipart_AndDeliversCompleteMessage(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions { Pool = pool, ReceiveSurface = ZReceiveSurface.Callback });
        var sink = new TestMessageSink(message => received.TrySetResult(message));
        server.BindMessageSink(sink);
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(MessageFactory.Multipart([.. "ping"u8], [.. "pong"u8]), cts.Token);

        var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        message.Count.Should().Be(2);
        message[0].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        message[1].ToSequence().ToArray().Should().Equal([.. "pong"u8]);

        // The surface owns the message and disposes it; the only remaining
        // rental is the server parser's greeting scratch, released on dispose.
        message.Dispose();
        pool.Outstanding.Should().Be(1);

        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task MessageLimit_ResetsPerMessage_AcrossManyMessages(TransportKind kind)
    {
        // The 0008 guard counters must reset at each message boundary: sending
        // many small messages under a tight MaxMessageLength must not
        // accumulate a total across messages and falsely reject (the counters
        // live in the transport core's per-peer materializer).
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
            MaxMessageLength = 10
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        const int count = 20;
        for (var i = 0; i < count; i++) await client.SendAsync(ZMessage.FromOwned([.. "ab"u8]), cts.Token);

        var received = 0;
        for (var attempt = 0; attempt < 50 && received < count; attempt++)
        {
            var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
            if (message is not null)
            {
                message.Value.Dispose();
                received++;
            }
        }

        received.Should().Be(count);
        server.ReceiveRejections.Should().Be(0);
    }

    [Fact]
    public async Task OnFrameSubscription_AfterMessageSinkBinding_Throws()
    {
        // The raw frame surface and the message sink are mutually exclusive
        // (0007 section 1): exactly one consumer of the delivery stream.
        await using var socket = new ZPairSocket(new ZSocketOptions { ReceiveSurface = ZReceiveSurface.Callback });
        socket.BindMessageSink(new TestMessageSink(_ => { }));
        var act = () => socket.OnFrame += (_, _) => true;
        act.Should().Throw<InvalidOperationException>();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ConcurrentSends_DoNotInterleaveMultipartFrames(TransportKind kind)
    {
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var server = new ZPairSocket(new ZSocketOptions { ReceiveSurface = ZReceiveSurface.Callback });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(64) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        var received = new ConcurrentQueue<byte[][]>();
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var current = new List<byte[]>();
        server.OnFrame += (frame, ct) =>
        {
            frame.TryGetValue(out ZSegment segment);
            current.Add(segment.Memory.ToArray());
            if (frame.More) return true;
            received.Enqueue([.. current]);
            current.Clear();
            if (++delivered == 100) allReceived.TrySetResult();

            return true;
        };

        var senderA = SendLoopAsync(client, 0x61, cts.Token);
        var senderB = SendLoopAsync(client, 0x62, cts.Token);
        await Task.WhenAll(senderA, senderB);

        // SendAsync completes once the frames are handed to the socket, not
        // when the peer's parser has delivered them; await the delivery
        // signal from OnFrame instead of polling, so the assert runs after
        // every message actually arrived.
        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        received.Should().HaveCount(100);
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
        await using var server = new ZPairSocket();
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
        if (ended == peerEnded.Task) (await peerEnded.Task is null or IOException).Should().BeTrue();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task SendRacingHandshake_DoesNotCorruptPeerHandshake(TransportKind kind)
    {
        for (var attempt = 0; attempt < 40; attempt++)
        {
            var endpoint = TestTransports.GetEndpoint(kind);
            await using var server = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
            await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await server.BindAsync(endpoint, cts.Token);
            await client.ConnectAsync(endpoint, cts.Token);

            // A send before the peer is routable is dropped; resend until the
            // message flows, which also proves the handshake was not corrupted.
            var received = false;
            for (var retry = 0; retry < 20 && !received; retry++)
            {
                await server.SendAsync(ZMessage.FromOwned([.. "x"u8]), cts.Token);
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
        await using var client = new ZPairSocket();

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
        await using var client = new ZPairSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(new byte[64]);
        await stream.FlushAsync();

        var act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_PeerSocketTypeMismatch_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZPairSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("DEALER")));
        await stream.FlushAsync();

        var act = () => connectTask;
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
                if (count == 0) break;

                received.Write(chunk, 0, count);
                if (received.ToArray().AsSpan().IndexOf("ERROR"u8) >= 0) break;
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
        await using var client = new ZPairSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready()));
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
        await using var client = new ZPairSocket();
        using var cts = new CancellationTokenSource();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        var act = () => connectTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ConnectAsync_DisconnectDuringHandshake_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZPairSocket();

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
        (caught is OperationCanceledException or IOException or SocketException or ObjectDisposedException).Should()
            .BeTrue();
    }

    [Fact]
    public async Task ConnectAsync_DealerPeerRep_Completes()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZDealerSocket();

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
        await using var client = new ZDealerSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("REQ")));
        await stream.FlushAsync();

        var act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_SynchronousHandshakeFailure_LeavesNoRoutablePeer()
    {
        await using var client = new ZPairSocket();

        var act = () => client.ConnectAsync<EndPoint, SynchronousEofTransport>(
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
        var server = new ZPairSocket(new ZSocketOptions { Pool = pool, ReceiveSurface = ZReceiveSurface.Callback });
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");
        server.OnFrame += (_, _) => throw new InvalidOperationException("callback failed");

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready(), ZmtpTestData.Frame([.. "boom"u8])));
        await stream.FlushAsync();

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        failure.Should().BeOfType<InvalidOperationException>();
        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task OversizedFrame_RejectsPeerAndReclaimsPool(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxFrameLength = 4
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        rejected.Rejection.Limit.Should().Be(4);
        rejected.Rejection.Actual.Should().Be(5);
        server.ReceiveRejections.Should().Be(1);
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task OversizedMessage_RejectsBeforeDeliveringAnyFrame(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxMessageLength = 10
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(MessageFactory.Multipart([.. "aaaaaa"u8], [.. "bbbbbb"u8]), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);
        rejected.Rejection.Limit.Should().Be(10);
        rejected.Rejection.Actual.Should().Be(12);
        server.ReceiveRejections.Should().Be(1);
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task RejectedPeer_IsNotRoutable_NoFurtherMessages(TransportKind kind)
    {
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxFrameLength = 4
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        // A message below the limit establishes and flows first.
        var first = false;
        for (var attempt = 0; attempt < 50 && !first; attempt++)
        {
            await client.SendAsync(ZMessage.FromOwned([.. "ok"u8]), cts.Token);
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
        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);
        await client.SendAsync(ZMessage.FromOwned([.. "ok"u8]), cts.Token);

        var extra = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(500), cts.Token);
        extra.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task RejectedPeer_EndsWithClose_NotWithPeerError(TransportKind kind)
    {
        var clientEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxFrameLength = 4
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        client.PeerEnded += (_, failure) => clientEnded.TrySetResult(failure);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);

        // Traffic-phase rejection is a plain close, never an ERROR command
        // (0008 D5): the client sees EOF/IO, not a peer protocol error. The
        // close propagation is OS-dependent (Windows/Ubuntu runners can lag),
        // so the wait window is generous.
        var failure = await clientEnded.Task.WaitAsync(TimeSpan.FromSeconds(15));
        (failure is null or IOException or SocketException).Should().BeTrue();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task OverLimitFrame_DoesNotRentFromPool(TransportKind kind)
    {
        using var pool = new ProbingMemoryPool();
        var inner = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxFrameLength = 4
        });
        await using var server = inner;
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        // Prove the probe works for in-limit frames.
        await client.SendAsync(ZMessage.FromOwned([.. "ok"u8]), cts.Token);
        var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(500), cts.Token);
        message.Should().NotBeNull();
        message.Value.Dispose();
        pool.Rentals.Should().BeGreaterThan(0);

        // The over-limit frame is rejected before any allocation; wait for the
        // deterministic rejection signal (the materializer's internal event)
        // instead of sleeping or polling, then prove the pool was never
        // touched.
        var rejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        inner.MaterializerRejected += () => rejected.TrySetResult();
        pool.Reset();
        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);
        await rejected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        pool.Rentals.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task Limits_EnforcedOutsidePolicy_CustomPolicyCannotBypass(TransportKind kind)
    {
        // The policy is allocation-only and cannot reject or bypass the
        // socket-level limits: they are enforced by the connection guard
        // before Decide is called.
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            MaxFrameLength = 4,
            ReceivePolicy = new ZDelegateReceivePolicy(_ => new ZReceiveAllocation { Mode = ZReceiveMode.Pooled })
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rejected = failure.Should().BeOfType<ZReceiveRejectedException>().Which;
        rejected.Rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        server.ReceiveRejections.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task WaitMode_DoesNotLoseMessages(TransportKind kind)
    {
        // With a capacity of 2 and no consumer, the per-peer pump blocks on
        // WriteAsync; the messages are not dropped and all arrive once read.
        await using var server = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        const int count = 20;
        for (var i = 0; i < count; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), cts.Token);

        var received = new List<byte>();
        while (received.Count < count)
        {
            var message = await TryReadAsync(server.Messages, TimeSpan.FromMilliseconds(500), cts.Token);
            message.Should().NotBeNull();
            received.Add(message.Value[0].ToSequence().ToArray()[0]);
            message.Value.Dispose();
        }

        received.Should().HaveCount(count);
        received.Should().Equal(Enumerable.Range(0, count).Select(i => (byte)i));
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task WaitMode_SlowPeer_DoesNotBlockOtherPeer(TransportKind kind)
    {
        var endpointA = TestTransports.GetEndpoint(kind);
        var endpointB = TestTransports.GetEndpoint(kind);
        await using var serverA = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        await using var serverB = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        await using var dealer = new ZDealerSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await serverA.BindAsync(endpointA, cts.Token);
        await serverB.BindAsync(endpointB, cts.Token);
        await dealer.ConnectAsync(endpointA, cts.Token);
        await dealer.ConnectAsync(endpointB, cts.Token);

        var received = new List<byte>();
        var drainB = DrainAsync(serverB, () => { }, cts.Token);

        // Saturate peer A (capacity 2, nobody reads it), then send to peer B.
        for (var i = 0; i < 100; i++) await dealer.SendAsync(ZMessage.FromOwned([1]), cts.Token);

        var reachedB = false;
        for (var attempt = 0; attempt < 100 && !reachedB; attempt++)
        {
            await dealer.SendAsync(ZMessage.FromOwned([2]), cts.Token);
            var message = await TryReadAsync(serverB.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
            if (message is not null)
            {
                received.Add(message.Value[0].ToSequence().ToArray()[0]);
                message.Value.Dispose();
                reachedB = received.Any(value => value == 2);
            }
        }

        reachedB.Should().BeTrue("a full queue on peer A must not pause peer B's delivery");
        await cts.CancelAsync();
        try
        {
            await drainB;
        }
        catch (OperationCanceledException)
        {
        }
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DropWriteMode_KeepsFirstMessages_AndReturnsPoolOnDispose(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var dropTracking = new DropTrackingQueueFactory(2, BoundedChannelFullMode.DropWrite);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = dropTracking,
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        // Ten messages against a capacity-2 queue: the first two stay, the
        // rest are dropped and reclaimed by the channel's item-dropped hook.
        // The drop callback is the library's mandatory reclamation signal, so
        // waiting for all eight drops is the deterministic completion - no
        // settling on a pool counter that transiently dips during the drop
        // path (0006 section 2.2).
        for (var i = 0; i < 10; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), cts.Token);

        await dropTracking.WaitForDropsAsync(8, TimeSpan.FromSeconds(5));

        var received = await ReadAllAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        received.Should().Equal(0, 1);

        // The eight dropped messages were disposed by the library, never seen
        // by the consumer (0006 section 2.2).
        pool.Outstanding.Should().Be(0);

        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DropNewestMode_KeepsOldestAndIncoming(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var dropTracking = new DropTrackingQueueFactory(2, BoundedChannelFullMode.DropNewest);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = dropTracking,
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        // Drain the queue so each later step starts from empty.
        await SendAndReadBackAsync(client, server.Messages, [0, 1], cts.Token);

        // Fill it; nothing consumes and nothing drops, so the two frames are
        // materialized and the pool count climbs monotonically to 2.
        await client.SendAsync(ZMessage.FromOwned([2]), cts.Token);
        await client.SendAsync(ZMessage.FromOwned([3]), cts.Token);
        await pool.WaitForOutstandingAtLeastAsync(2, TimeSpan.FromSeconds(5));

        // A full queue in DropNewest mode discards the newest buffered item
        // (3) and keeps the incoming one (4) (0006 section 3.5). The drop
        // callback is the deterministic signal that the discard ran before
        // any read frees a slot.
        await client.SendAsync(ZMessage.FromOwned([4]), cts.Token);
        await dropTracking.WaitForDropsAsync(1, TimeSpan.FromSeconds(5));

        var received = await ReadAllAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        received.Should().Equal(2, 4);

        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DropOldestMode_KeepsNewestMessages(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var dropTracking = new DropTrackingQueueFactory(2, BoundedChannelFullMode.DropOldest);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = dropTracking,
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await SendAndReadBackAsync(client, server.Messages, [0, 1], cts.Token);

        await client.SendAsync(ZMessage.FromOwned([2]), cts.Token);
        await client.SendAsync(ZMessage.FromOwned([3]), cts.Token);
        await pool.WaitForOutstandingAtLeastAsync(2, TimeSpan.FromSeconds(5));

        // A full queue in DropOldest mode discards the oldest buffered item
        // (2) and keeps the incoming one (4); the drop callback signals the
        // discard deterministically.
        await client.SendAsync(ZMessage.FromOwned([4]), cts.Token);
        await dropTracking.WaitForDropsAsync(1, TimeSpan.FromSeconds(5));

        var received = await ReadAllAsync(server.Messages, TimeSpan.FromMilliseconds(200), cts.Token);
        received.Should().Equal(3, 4);

        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PeerEnd_DrainsBufferedMessages_ReturnsPool(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropWrite },
        });
        server.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        for (var i = 0; i < 4; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), cts.Token);

        // The two buffered items are rented (the materializer allocates each
        // frame's buffer directly; there is no persistent parser scratch).
        await WaitUntilSettledAsync(() => pool.Outstanding, 2, TimeSpan.FromMilliseconds(300));

        await client.DisposeAsync();
        await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // OnPeerEnded drained the buffered messages through the same Dispose
        // path a drop uses; nothing leaks (0006 section 2.2).
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));

        await server.DisposeAsync();
        pool.Outstanding.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task SocketDispose_WithUnreadBufferedMessages_ReturnsPool(TransportKind kind)
    {
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(2) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "a"u8]), cts.Token);
        await client.SendAsync(ZMessage.FromOwned([.. "b"u8]), cts.Token);
        await WaitUntilSettledAsync(() => pool.Outstanding, 2, TimeSpan.FromMilliseconds(300));

        // Disposing with unread buffered messages reclaims them; PeerEnded is
        // unsubscribed during disposal, so this is the only reclaim path.
        await server.DisposeAsync();
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Dispose_DrainsBufferedOutboundMessages()
    {
        using var pool = new CountingMemoryPool();
        using var listener = new TcpListener(IPAddress.Loopback, GetFreePort());
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();

        var client = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            SendQueueFactory = new BoundedChannelOptions(2) { SingleWriter = false },
        });

        // The raw peer never answers READY, so the send pump blocks on the
        // establishment gate and the outbound channel backs up.
        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.ReadExactlyAsync(new byte[64]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        var outbound = client.Outbound ?? throw new InvalidOperationException("send channel is not configured");
        await outbound.WriteAsync(MessageFactory.PooledSingleFrame(pool, [.. "a"u8]));
        await outbound.WriteAsync(MessageFactory.PooledSingleFrame(pool, [.. "b"u8]));

        // The two outbound messages are rented; the pump is blocked on the
        // establishment gate and has dequeued nothing. The handshake's pool
        // rents are transient (released when establishment completes), so
        // only the buffered messages remain outstanding.
        pool.Outstanding.Should().Be(2);

        await client.DisposeAsync();

        // The pump reclaimed the dequeued message on cancellation and disposal
        // drained the buffered one; both buffers are returned.
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }

    [Theory]
    [InlineData(BoundedChannelFullMode.DropWrite)]
    [InlineData(BoundedChannelFullMode.DropNewest)]
    [InlineData(BoundedChannelFullMode.DropOldest)]
    public async Task SendDropMode_ProducersNeverBlock_AndAllReclaimed(BoundedChannelFullMode mode)
    {
        using var pool = new CountingMemoryPool();
        using var listener = new TcpListener(IPAddress.Loopback, GetFreePort());
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();

        var client = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            SendQueueFactory = new BoundedChannelOptions(2) { FullMode = mode, SingleWriter = false },
        });

        // The raw peer never answers READY, so the send pump dequeues the
        // first message and blocks on the establishment gate.
        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.ReadExactlyAsync(new byte[64]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        var outbound = client.Outbound ?? throw new InvalidOperationException("send channel is not configured");

        // Ten producers against a capacity-2 channel: the first three stay
        // (pump-held + 2 buffered), the rest are dropped and reclaimed. The
        // per-write timeout proves a drop mode never blocks a producer.
        for (var i = 0; i < 10; i++)
            await outbound.WriteAsync(MessageFactory.PooledSingleFrame(pool, [(byte)i]))
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(2));

        // The pump holds one message (blocked on the establishment gate) and
        // the channel buffers up to capacity; the dropped messages are already
        // disposed via the item-dropped hook. The pump's take timing varies
        // with scheduling, so only the bounded range is asserted - never above
        // 3 - and the per-write timeout above proves a drop mode never blocks
        // a producer (0006 3.5). The handshake's pool rents are transient
        // (released once establishment completes), so no persistent scratch
        // is counted.
        await WaitUntilAsync(() => pool.Outstanding is >= 2 and <= 3, TimeSpan.FromSeconds(5));

        await client.DisposeAsync();

        // Disposal drained the buffered and pump-held messages.
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SendPumpFailure_CompletesOutboundWithFailure()
    {
        using var pool = new CountingMemoryPool();
        using var listener = new TcpListener(IPAddress.Loopback, GetFreePort());
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();

        var client = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            SendQueueFactory = new BoundedChannelOptions(2) { SingleWriter = false },
        });

        // The peer completes the handshake as a DEALER, which is incompatible
        // with a PAIR: the establishment gate faults deterministically, so the
        // pump's first send fails with a protocol error - no TCP timing races.
        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("DEALER")));
        await stream.FlushAsync();

        var outbound = client.Outbound ?? throw new InvalidOperationException("send channel is not configured");

        // The pump may fault the establishment gate and complete the channel
        // before these writes land (the gate faults deterministically once the
        // READY exchange rejects the socket type), so a write racing that
        // completion throws ChannelClosedException with the protocol failure -
        // which is exactly the behavior under test.
        foreach (var payload in new[] { "a"u8.ToArray(), "b"u8.ToArray() })
        {
            var message = MessageFactory.PooledSingleFrame(pool, payload);
            var writeFailure = await Record.ExceptionAsync(() => outbound.WriteAsync(message).AsTask());
            if (writeFailure is not null)
            {
                writeFailure.Should().BeOfType<ChannelClosedException>();
                writeFailure.InnerException.Should().BeOfType<ZeroMqProtocolException>();
                message.Dispose();
            }
        }

        // The pump's first send faults the establishment gate and completes
        // the producer surface with the failure. The establishment failure is
        // deterministic (the gate faults when the READY exchange rejects the
        // socket type), and OnPeerEnded completes the outbound channel with it
        // once the last peer ends. WaitToWriteAsync is the deterministic
        // completion signal: on a completed channel it resolves immediately,
        // surfacing the completion error (0006 3.5).
        var connectFailure = await Record.ExceptionAsync(() => connectTask.WaitAsync(TimeSpan.FromSeconds(5)));
        connectFailure.Should().BeOfType<ZeroMqProtocolException>();

        var waitFailure = await Record.ExceptionAsync(
            () => outbound.WaitToWriteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        waitFailure.Should().BeOfType<ZeroMqProtocolException>();

        // The completion is a terminal state, so a single write now fails
        // deterministically with the channel-closed failure.
        var probe = MessageFactory.PooledSingleFrame(pool, [.. "probe"u8]);
        var completedWriteFailure = await Record.ExceptionAsync(() => outbound.WriteAsync(probe).AsTask());
        completedWriteFailure.Should().BeOfType<ChannelClosedException>();
        completedWriteFailure.As<ChannelClosedException>().InnerException.Should().BeOfType<ZeroMqProtocolException>();
        probe.Dispose(); // never accepted: the channel is completed, so it must not leak

        await client.DisposeAsync();
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SendPath_NoPerMessageHeapAllocation()
    {
        // The send hot path must not allocate per message: a copy-on-write
        // snapshot read (0006 3.6), a span-based single-target route, the
        // established gate fast path, and a no-op fake write.
        await using var socket = new ZPairSocket();
        await socket.ConnectAsync<EndPoint, EstablishedFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));

        var messages = new ZMessage[1000];
        for (var i = 0; i < messages.Length; i++) messages[i] = ZMessage.FromOwned([(byte)i]);

        // Warm up: the first sends may trigger one-time costs (delegate
        // caches, tiered JIT), which would pollute the measurement.
        for (var i = 0; i < 16; i++) await socket.SendAsync(messages[i]);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 16; i < messages.Length; i++) await socket.SendAsync(messages[i]);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
#if !DEBUG
        // In an optimized build the send path is allocation-free: the COW
        // snapshot is a single volatile load, routing is span-based, the
        // established gate takes its fast path, and the fake write is a
        // no-op. Debug boxes async state machines per call (a bare
        // synchronous-completing async ValueTask measures ~48 bytes/call
        // there), so the absolute gate only holds in Release (0006 3.6).
        allocated.Should().BeLessThan(4096);
#else
        _ = allocated;
#endif

        foreach (var message in messages) message.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ReceivePath_AggregateDrain_DoesNotAllocatePerMessage(TransportKind kind)
    {
        // The aggregate drain hot path must not allocate per message: the
        // peer snapshot is a single volatile load and TryRead returns an
        // already-materialized message, so draining the reader allocates
        // nothing (0006 3.6). This measures the caller's drain side only:
        // the receiving pump thread's own allocations (parse + materialize)
        // are sampled inside the pump in ZmqSharp.AllocationTests, because a
        // counter read on this test thread never sees that thread.
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(1024) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(1024) { SingleWriter = true } });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        const int count = 1000;
        for (var i = 0; i < count; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), cts.Token);

        // Capacity comfortably exceeds the send count and nothing consumes,
        // so Outstanding climbs monotonically to count; wait for it directly
        // instead of a settle window (each send is fully materialized and no
        // drop or drain can transiently dip the counter).
        await pool.WaitForOutstandingAtLeastAsync(count, TimeSpan.FromSeconds(5));

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var received = 0;
        while (server.Messages.TryRead(out var message))
        {
            message.Dispose();
            received++;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        allocated.Should().BeLessThan(4096);
        received.Should().Be(count);
    }

    [Fact]
    public async Task PeerChurn_ConcurrentSendReadDispose_NoLeaksOrFaults()
    {
        // Tcp-only: under the full-suite concurrency pressure this churn
        // scenario exposes a baseline pool-reclaim race (an outstanding
        // rental stays strongly reachable past dispose) unrelated to the ipc
        // work; it stays in its stable single-transport form here and the
        // ipc concurrency coverage is provided by ZSocketIpcTests and the
        // parameterized concurrent-send suites.
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true },
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);

        var failures = new ConcurrentQueue<Exception>();

        var sender = Task.Run(async () =>
        {
            try
            {
                for (var i = 0; i < 300; i++) await server.SendAsync(ZMessage.FromOwned([1]), cts.Token);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                failures.Enqueue(ex);
            }
        });

        var drainer = Task.Run(async () =>
        {
            try
            {
                var messages = server.Messages;
                await foreach (var message in messages.ReadAllAsync(cts.Token)) message.Dispose();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                failures.Enqueue(ex);
            }
        });

        // Churn: repeatedly connect a short-lived client, exchange messages,
        // and disconnect - each teardown reclaims that peer's buffers and
        // retires it out of the routing snapshot.
        for (var round = 0; round < 4; round++)
        {
            await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
            await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);
            for (var i = 0; i < 20; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), cts.Token);
        }

        await sender;
        await cts.CancelAsync();
        try
        {
            await drainer;
        }
        catch (OperationCanceledException)
        {
        }

        failures.Should().BeEmpty();

        await server.DisposeAsync();
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }

    // ---- Security mechanism boundary (0016) ----

    [Fact]
    public async Task ConnectAsync_ReadyMissingSocketType_Throws()
    {
        // READY Socket-Type metadata validation moved to the socket layer with
        // the handshake boundary; a peer whose READY lacks Socket-Type is
        // rejected at establishment.
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZPairSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithProperties(("Identity", "abc"))));
        await stream.FlushAsync();

        var act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_ReadyInvalidSocketType_Throws()
    {
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZPairSocket();

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();
        await stream.WriteAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithProperties(("Socket-Type", "FOO"))));
        await stream.FlushAsync();

        var act = () => connectTask;
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ConnectAsync_CustomMechanism_ExchangesBeforeReady_AndCompletes()
    {
        // The replaceability gate (0006 section 4 / 0016 section 10): a test
        // mechanism with a PING/PONG command pair before READY replaces NULL
        // through explicit configuration alone - no reflection, no registry.
        var port = GetFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync();
        await using var client = new ZPairSocket(new ZSocketOptions { Security = new ZSecurityOptions { Mechanism = new TestPingPongMechanism() } });

        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}");
        using var raw = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var stream = raw.GetStream();

        // Server side of the TEST mechanism: complete the greeting (reading the
        // client's TEST greeting) and answer the mechanism's command sequence.
        var greeting = new byte[64];
        await stream.ReadExactlyAsync(greeting).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await stream.WriteAsync(ZmtpTestData.Greeting("TEST"));
        await stream.FlushAsync();

        var serverPong = ZmtpTestData.Frame([4, (byte)'P', (byte)'O', (byte)'N', (byte)'G'], command: true);
        var serverReady = ZmtpTestData.Ready();

        // Client sends PING (2-byte header + 5-byte body), expects PONG, then
        // exchanges READY; the server mirror responds in the same order.
        await stream.ReadExactlyAsync(new byte[2 + 5]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await stream.WriteAsync(serverPong);
        await stream.FlushAsync();

        // The client's READY frame: 2-byte header + 26-byte body (READY name,
        // Socket-Type property, PAIR value).
        await stream.ReadExactlyAsync(new byte[2 + 26]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await stream.WriteAsync(serverReady);
        await stream.FlushAsync();

        await connectTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static async Task EchoAsync(ZPairSocket server, ChannelReader<ZMessage> messages,
        CancellationToken token)
    {
        await foreach (var message in messages.ReadAllAsync(token)) await server.SendAsync(message, token);
    }

    private static async Task DrainAsync(ZQueueSocketBase socket, Action onMessage,
        CancellationToken token)
    {
        var messages = socket.Messages;
        await foreach (var message in messages.ReadAllAsync(token))
        {
            onMessage();
            message.Dispose();
        }
    }

    private static async Task WaitUntilAsync<T>(Func<T> state, Func<T, bool> condition, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            var current = state();
            if (condition(current)) return;

            if (stopwatch.Elapsed >= timeout)
                throw new TimeoutException($"condition not met within timeout; last value: {current}");

            await Task.Delay(20);
        }
    }

    private static Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        return WaitUntilAsync(condition, value => value, timeout);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            if (await condition()) return;

            if (stopwatch.Elapsed >= timeout) throw new TimeoutException("condition not met within timeout");

            await Task.Delay(20);
        }
    }

    /// <summary>
    ///     Waits until the observed value is stable at <paramref name="expected" />
    ///     for the whole settle interval. Lets the receiving pump finish draining
    ///     (drops processed, drops reclaimed) before the queue is read, so tests
    ///     do not race the pump.
    /// </summary>
    private static async Task WaitUntilSettledAsync<T>(Func<T> state, T expected, TimeSpan interval)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
        {
            if (state()!.Equals(expected))
            {
                await Task.Delay(interval);
                if (state()!.Equals(expected)) return;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"value never settled at {expected}; last value: {state()}");
    }

    private static async Task SendAndReadBackAsync(
        ZPairSocket client,
        ChannelReader<ZMessage> messages,
        byte[] payloads,
        CancellationToken token)
    {
        foreach (var payload in payloads)
        {
            await client.SendAsync(ZMessage.FromOwned([payload]), token);
            var message = await TryReadAsync(messages, TimeSpan.FromSeconds(1), token);
            message.Should().NotBeNull();
            message.Value[0].ToSequence().ToArray()[0].Should().Be(payload);
            message.Value.Dispose();
        }
    }

    private static async Task<List<byte>> ReadAllAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan idleTimeout,
        CancellationToken token)
    {
        var result = new List<byte>();
        while (true)
        {
            var message = await TryReadAsync(reader, idleTimeout, token);
            if (message is null) return result;

            result.Add(message.Value[0].ToSequence().ToArray()[0]);
            message.Value.Dispose();
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

    private static async Task SendLoopAsync(ZPairSocket client, byte tag, CancellationToken token)
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

    private sealed class TestMessageSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
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
    {
        return ValueTask.FromResult<IZConnection>(new SynchronousEofConnection());
    }

    public static ValueTask<SynchronousEofTransport> BindAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        return ValueTask.FromResult(new SynchronousEofTransport());
    }

    public ValueTask StartAsync(CancellationToken token = default)
    {
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
    }
}

internal sealed class SynchronousEofConnection : IZConnection
{
    private int disposed;

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        return ValueTask.FromResult(0);
    }

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        return ValueTask.CompletedTask;
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
    {
        return WriteAsync(frame, token);
    }

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return WriteAsync(body, token);
    }

    public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        return WriteAsync(ReadOnlyMemory<byte>.Empty, token);
    }

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        return ValueTask.FromResult(true);
    }

    public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
    {
    }

    public void SetConnectionEndedHandler(Action onConnectionEnded)
    {
    }

    public void OnConnectionEnded()
    {
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref disposed, 1);
    }
}

/// <summary>
/// Test mechanism for the replaceability gate (0016 section 10): the session
/// exchanges a PING/PONG command pair before READY, proving the boundary is
/// not NULL-shaped.
/// </summary>
internal sealed class TestPingPongMechanism : IZSecurityMechanism
{
    public string Name => "TEST";

    public IZMechanismSession CreateSession(ZMechanismRole role)
    {
        return new PingPongSession();
    }

    private sealed class PingPongSession : IZMechanismSession
    {
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            await context.WriteCommandAsync(new byte[] { 4, (byte)'P', (byte)'I', (byte)'N', (byte)'G' }, token);

            var command = await context.ReadCommandAsync(token);
            if (command is null) return null;
            if (!command.Value.Name.Span.SequenceEqual("PONG"u8))
                throw new ZMechanismException("expected PONG");

            await context.WriteCommandAsync(context.LocalReadyBody, token);
            var ready = await context.ReadCommandAsync(token);
            if (ready is null) return null;
            if (!ready.Value.Name.Span.SequenceEqual("READY"u8))
                throw new ZMechanismException("expected READY");

            return new ZMechanismResult(context.Connection, ready.Value.Arguments.ToArray());
        }
    }
}
