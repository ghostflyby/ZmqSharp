using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
///     PAIR interop with the NetMQ libzmq-compatible implementation over TCP,
///     in both directions (0006 section 5): greeting/READY, short/long/multipart
///     messages, partial reads (long frames), and peer close.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class PairInteropTests
{
    [Fact]
    public async Task ZmqSharpServer_NetMQClient_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var server = new ZQueueSocket<ZPairSocket>(new ZPairSocket(), new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await server.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // ZmqSharp -> NetMQ.
        await server.SendAsync(ZMessage.FromOwned([.. "ping"u8]), cts.Token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(5));
        received.Should().Equal([.. "ping"u8]);

        // NetMQ -> ZmqSharp.
        peer.SendFrame([.. "pong"u8]);
        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), cts.Token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "pong"u8]);
        message.Value.Dispose();
    }

    [Fact]
    public async Task NetMQServer_ZmqSharpClient_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var client = new ZQueueSocket<ZPairSocket>(new ZPairSocket(), new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // NetMQ -> ZmqSharp.
        peer.SendFrame([.. "hello"u8]);
        var message = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), cts.Token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "hello"u8]);
        message.Value.Dispose();

        // ZmqSharp -> NetMQ.
        await client.SendAsync(ZMessage.FromOwned([.. "world"u8]), cts.Token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(5));
        received.Should().Equal([.. "world"u8]);
    }

    [Fact]
    public async Task Multipart_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZQueueSocket<ZPairSocket>(new ZPairSocket(), new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // ZmqSharp -> NetMQ multipart.
        await socket.SendAsync(MessageFactory.Multipart([.. "a"u8], [.. "b"u8], [.. "c"u8]), cts.Token);
        var frames = new NetMQMessage();
        peer.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref frames).Should().BeTrue();
        frames.Should().NotBeNull();
        frames.FrameCount.Should().Be(3);
        frames[0].ToByteArray().Should().Equal([.. "a"u8]);
        frames[1].ToByteArray().Should().Equal([.. "b"u8]);
        frames[2].ToByteArray().Should().Equal([.. "c"u8]);

        // NetMQ -> ZmqSharp multipart.
        var reply = new NetMQMessage();
        reply.Append(new NetMQFrame([.. "x"u8]));
        reply.Append(new NetMQFrame([.. "y"u8]));
        peer.SendMultipartMessage(reply);

        var message = await ReadMessageAsync(socket.Messages, TimeSpan.FromSeconds(5), cts.Token);
        message.Should().NotBeNull();
        message.Value.Count.Should().Be(2);
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "x"u8]);
        message.Value[1].ToSequence().ToArray().Should().Equal([.. "y"u8]);
        message.Value.Dispose();
    }

    [Fact]
    public async Task LongFrame_BothDirections()
    {
        var payload = new byte[100_000];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 251);

        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZQueueSocket<ZPairSocket>(new ZPairSocket(), new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // ZmqSharp -> NetMQ long frame (crosses many TCP segments).
        await socket.SendAsync(ZMessage.FromOwned(payload), cts.Token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(10));
        received.Should().Equal(payload);

        // NetMQ -> ZmqSharp long frame.
        peer.SendFrame(payload);
        var message = await ReadMessageAsync(socket.Messages, TimeSpan.FromSeconds(10), cts.Token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal(payload);
        message.Value.Dispose();
    }

    [Fact]
    public async Task NetMQPeerClose_RaisesPeerEnded()
    {
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZQueueSocket<ZPairSocket>(new ZPairSocket(), new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        socket.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // A graceful NetMQ close surfaces as a clean EOF on our side.
        peer.Close();

        var failure = await peerEnded.Task.WaitAsync(TimeSpan.FromSeconds(10));
        (failure is null or IOException or SocketException).Should().BeTrue();
    }

    private static async Task<ZMessage?> ReadMessageAsync(
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
}
