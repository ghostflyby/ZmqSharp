using System.Buffers;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// End-to-end tests of <see cref="ZSocketConnection"/> (0015 section 4): a
/// raw-socket pair completing the NULL handshake, with the receiving side
/// parsed by <see cref="ZmtpParser"/>. Covers the direct
/// <see cref="Socket.ReceiveAsync"/> read path and the buffer-list scatter
/// write path (one system call per frame). The parser pump runs in the
/// background (on a live socket it only stops when the peer closes) and the
/// test drives the send side. The bulk regression for the connection swap
/// lives in the transport-parameterized suites (ZSocketTests, ZReqRepTests,
/// ...), which now run every scenario over ZSocketConnection.
/// </summary>
public sealed class ZSocketConnectionTests
{
    [Fact]
    public async Task SendFrameAsync_ReachesPeerAsExactFrame()
    {
        var (client, server) = await OpenPairAsync();
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder));

            var session = await ZmtpTestRunner.EstablishAsync(client);
            session.Should().NotBeNull();
            using var sender = new ZmtpSession(client);

            await sender.SendFrameAsync("hello"u8.ToArray(), more: false);

            await recorder.FirstFrameAsync.WaitAsync(TimeSpan.FromSeconds(5));
            recorder.Frames.Should().HaveCount(1);
            recorder.Frames[0].Should().Equal([.. "hello"u8]);
        }
    }

    [Fact]
    public async Task SegmentedMessage_RoundTripsAsOneFrame_OverScatterWrite()
    {
        var (client, server) = await OpenPairAsync();
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder));

            var session = await ZmtpTestRunner.EstablishAsync(client);
            session.Should().NotBeNull();
            using var sender = new ZmtpSession(client);

            // A multi-segment frame exercises the buffer-list scatter write:
            // header + each segment, sent with one SendAsync call.
            using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
            await sender.SendAsync(message);

            await recorder.FirstFrameAsync.WaitAsync(TimeSpan.FromSeconds(5));
            recorder.Frames.Should().HaveCount(1);
            recorder.Frames[0].Should().Equal([.. "hello!"u8]);
        }
    }

    [Fact]
    public async Task LongFrame_OverRawSocketPair_UsesLongEncoding()
    {
        var (client, server) = await OpenPairAsync();
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder));

            var session = await ZmtpTestRunner.EstablishAsync(client);
            session.Should().NotBeNull();
            using var sender = new ZmtpSession(client);

            var payload = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
            await sender.SendFrameAsync(payload, more: false);

            await recorder.FirstFrameAsync.WaitAsync(TimeSpan.FromSeconds(5));
            recorder.Frames.Should().HaveCount(1);
            recorder.Frames[0].Should().Equal(payload);
        }
    }

    [Fact]
    public async Task ReadAsync_ReturnsPeerBytes_WithoutAStreamWrapper()
    {
        var (client, server) = await OpenPairAsync();
        using (client)
        using (server)
        {
            // Write raw bytes on the client socket and read them through the
            // connection's direct Socket.ReceiveAsync path.
            await client.WriteAsync("direct"u8.ToArray());

            var buffer = new byte[6];
            var read = await server.ReadAsync(buffer);
            read.Should().Be(6);
            buffer.Should().Equal([.. "direct"u8]);
        }
    }

    [Fact]
    public async Task EmptyBodyFrame_RoundTrips_OverSingleSegmentFastPath()
    {
        // An empty frame body falls back to the single-segment sequence form,
        // exercising the socket sink's single-buffer fast path (no scatter).
        var (client, server) = await OpenPairAsync();
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder));

            var session = await ZmtpTestRunner.EstablishAsync(client);
            session.Should().NotBeNull();
            using var sender = new ZmtpSession(client);

            await sender.SendFrameAsync(ReadOnlyMemory<byte>.Empty, more: false);

            await recorder.FirstFrameAsync.WaitAsync(TimeSpan.FromSeconds(5));
            recorder.Frames.Should().HaveCount(1);
            recorder.Frames[0].Should().BeEmpty();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LargeScatterWrite_WithSmallSocketBuffer_WritesTheCompleteFrame(bool ipc)
    {
        var path = TestTransports.IpcSocketPath("zmq-scatter-");
        EndPoint endpoint = ipc ? new UnixDomainSocketEndPoint(path) : new IPEndPoint(IPAddress.Loopback, 0);
        using var listener = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(endpoint);
        listener.Listen();
        using var client = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
        client.SendBufferSize = 4096;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await client.ConnectAsync(listener.LocalEndPoint ?? endpoint, timeout.Token);
            using var server = await listener.AcceptAsync(timeout.Token);
            using var writer = new ZSocketConnection(client);
            using var reader = new ZSocketConnection(server);
            using var session = new ZmtpSession(writer);
            var first = Enumerable.Repeat((byte)17, 700_000).ToArray();
            var second = Enumerable.Repeat((byte)23, 800_000).ToArray();
            using var message = MessageFactory.SegmentedFrame(first, second);
            var delivered = 0;
            using var parser = new ZmtpParser(reader, (frame, _) =>
            {
                var content = frame.ToSequence().ToArray();
                content.Length.Should().Be(first.Length + second.Length);
                content.AsSpan(0, first.Length).SequenceEqual(first).Should().BeTrue();
                content.AsSpan(first.Length).SequenceEqual(second).Should().BeTrue();
                delivered++;
                return ValueTask.FromResult(true);
            });
            var receiving = parser.ParseAsync(timeout.Token).AsTask();
            await session.SendAsync(message, timeout.Token);
            writer.Abort();
            await receiving.WaitAsync(timeout.Token);
            delivered.Should().Be(1);
        }
        finally { if (ipc) File.Delete(path); }
    }

    /// <summary>Opens a connected raw-socket pair wrapped in ZSocketConnection.</summary>
    private static async Task<(ZSocketConnection Client, ZSocketConnection Server)> OpenPairAsync()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await clientSocket.ConnectAsync(listener.LocalEndpoint);
        var serverSocket = await listener.AcceptSocketAsync();
        listener.Stop();
        return (new ZSocketConnection(clientSocket), new ZSocketConnection(serverSocket));
    }
}
