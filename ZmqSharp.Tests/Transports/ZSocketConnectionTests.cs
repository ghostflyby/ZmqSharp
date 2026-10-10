using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// End-to-end tests of <see cref="ZSocketConnection"/> (0015 section 4): a
/// raw-socket pair completing the NULL handshake, with the receiving side
/// parsed by <see cref="ZmtpParser"/>. Covers the direct
/// <see cref="Socket.ReceiveAsync(System.ArraySegment{byte})"/> read path and the buffer-list scatter
/// write path (one system call per frame). The parser pump runs in the
/// background (on a live socket it only stops when the peer closes) and the
/// test drives the send side. The bulk regression for the connection swap
/// lives in the transport-parameterized suites (ZSocketTests, ZReqRepTests,
/// ...), which now run every scenario over ZSocketConnection.
/// </summary>
public sealed class ZSocketConnectionTests
{
    [Fact(Timeout = 10_000)]
    public async Task SendFrameAsync_ReachesPeerAsExactFrame()
    {
        var token = TestContext.Current.CancellationToken;
        var (client, server) = await OpenPairAsync(token);
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder), token);

            var session = await ZmtpTestRunner.EstablishAsync(client);
            Assert.NotNull(session);
            using var sender = new ZmtpSession(client);

            await sender.SendFrameAsync("hello"u8.ToArray(), more: false, token: token);

            await recorder.FirstFrameAsync.WaitAsync(token);
            var frame = Assert.Single(recorder.Frames);
            Assert.Equal([.. "hello"u8], frame);
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task SegmentedMessage_RoundTripsAsOneFrame_OverScatterWrite()
    {
        var token = TestContext.Current.CancellationToken;
        var (client, server) = await OpenPairAsync(token);
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder), token);

            var session = await ZmtpTestRunner.EstablishAsync(client);
            Assert.NotNull(session);
            using var sender = new ZmtpSession(client);

            // A multi-segment frame exercises the buffer-list scatter write:
            // header + each segment, sent with one SendAsync call.
            using var message = MessageFactory.SegmentedFrame([.. "hel"u8], [.. "lo"u8], [.. "!"u8]);
            await sender.SendAsync(message, token);

            await recorder.FirstFrameAsync.WaitAsync(token);
            var frame = Assert.Single(recorder.Frames);
            Assert.Equal([.. "hello!"u8], frame);
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task LongFrame_OverRawSocketPair_UsesLongEncoding()
    {
        var token = TestContext.Current.CancellationToken;
        var (client, server) = await OpenPairAsync(token);
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder), token);

            var session = await ZmtpTestRunner.EstablishAsync(client);
            Assert.NotNull(session);
            using var sender = new ZmtpSession(client);

            var payload = Enumerable.Range(0, 300).Select(i => (byte)(i % 251)).ToArray();
            await sender.SendFrameAsync(payload, more: false, token: token);

            await recorder.FirstFrameAsync.WaitAsync(token);
            var frame = Assert.Single(recorder.Frames);
            Assert.Equal(payload, frame);
        }
    }

    [Fact]
    public async Task ReadAsync_ReturnsPeerBytes_WithoutAStreamWrapper()
    {
        var token = TestContext.Current.CancellationToken;
        var (client, server) = await OpenPairAsync(token);
        using (client)
        using (server)
        {
            // Write raw bytes on the client socket and read them through the
            // connection's direct Socket.ReceiveAsync path.
            await client.WriteAsync("direct"u8.ToArray(), token);

            var buffer = new byte[6];
            var read = await server.ReadAsync(buffer, token);
            Assert.Equal(6, read);
            Assert.Equal([.. "direct"u8], buffer);
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task EmptyBodyFrame_RoundTrips_OverSingleSegmentFastPath()
    {
        // An empty frame body falls back to the single-segment sequence form,
        // exercising the socket sink's single-buffer fast path (no scatter).
        var token = TestContext.Current.CancellationToken;
        var (client, server) = await OpenPairAsync(token);
        using (client)
        using (server)
        {
            var recorder = new FrameRecorder();
            _ = Task.Run(() => ZmtpTestRunner.RunParserAsync(server, recorder), token);

            var session = await ZmtpTestRunner.EstablishAsync(client);
            Assert.NotNull(session);
            using var sender = new ZmtpSession(client);

            await sender.SendFrameAsync(ReadOnlyMemory<byte>.Empty, more: false, token);

            await recorder.FirstFrameAsync.WaitAsync(token);
            var frame = Assert.Single(recorder.Frames);
            Assert.Empty(frame);
        }
    }

    [Theory(Timeout = 10_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LargeScatterWrite_WithSmallSocketBuffer_WritesTheCompleteFrame(bool ipc)
    {
        var token = TestContext.Current.CancellationToken;
        var path = TestTransports.IpcSocketPath("zmq-scatter-");
        EndPoint endpoint = ipc ? new UnixDomainSocketEndPoint(path) : new IPEndPoint(IPAddress.Loopback, 0);
        using var listener = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(endpoint);
        listener.Listen();
        using var client = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
        client.SendBufferSize = 4096;
        try
        {
            await client.ConnectAsync(listener.LocalEndPoint ?? endpoint, token);
            using var server = await listener.AcceptAsync(token);
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
                Assert.Equal(first.Length + second.Length, content.Length);
                Assert.True(content.AsSpan(0, first.Length).SequenceEqual(first));
                Assert.True(content.AsSpan(first.Length).SequenceEqual(second));
                delivered++;
                return ValueTask.FromResult(true);
            });
            var receiving = parser.ParseAsync(token).AsTask();
            await session.SendAsync(message, token);
            writer.Abort();
            await receiving.WaitAsync(token);
            Assert.Equal(1, delivered);
        }
        finally
        {
            if (ipc) File.Delete(path);
        }
    }

    /// <summary>Opens a connected raw-socket pair wrapped in ZSocketConnection.</summary>
    private static async Task<(ZSocketConnection Client, ZSocketConnection Server)> OpenPairAsync(CancellationToken token)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await clientSocket.ConnectAsync(listener.LocalEndpoint, token);
        var serverSocket = await listener.AcceptSocketAsync(token);
        listener.Stop();
        return (new ZSocketConnection(clientSocket), new ZSocketConnection(serverSocket));
    }
}
