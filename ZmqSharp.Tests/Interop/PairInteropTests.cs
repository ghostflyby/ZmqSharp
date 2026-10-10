using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
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
    [Fact(Timeout = 20_000)]
    public async Task ZmqSharpServer_NetMQClient_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var server = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        await server.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // ZmqSharp -> NetMQ.
        await server.SendAsync(ZMessage.FromOwned([.. "ping"u8]), token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(5));
        Assert.Equal([.. "ping"u8], received);

        // NetMQ -> ZmqSharp.
        peer.SendFrame([.. "pong"u8]);
        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(message);
        Assert.Equal([.. "pong"u8], message.Value[0].ToSequence().ToArray());
        message.Value.Dispose();
    }

    [Fact(Timeout = 20_000)]
    public async Task NetMQServer_ZmqSharpClient_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var client = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // NetMQ -> ZmqSharp.
        peer.SendFrame([.. "hello"u8]);
        var message = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(message);
        Assert.Equal([.. "hello"u8], message.Value[0].ToSequence().ToArray());
        message.Value.Dispose();

        // ZmqSharp -> NetMQ.
        await client.SendAsync(ZMessage.FromOwned([.. "world"u8]), token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(5));
        Assert.Equal([.. "world"u8], received);
    }

    [Fact(Timeout = 20_000)]
    public async Task Multipart_BothDirections()
    {
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // ZmqSharp -> NetMQ multipart.
        await socket.SendAsync(MessageFactory.Multipart([.. "a"u8], [.. "b"u8], [.. "c"u8]), token);
        var frames = new NetMQMessage();
        Assert.True(peer.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref frames));
        Assert.NotNull(frames);
        Assert.Equal(3, frames.FrameCount);
        Assert.Equal([.. "a"u8], frames[0].ToByteArray());
        Assert.Equal([.. "b"u8], frames[1].ToByteArray());
        Assert.Equal([.. "c"u8], frames[2].ToByteArray());

        // NetMQ -> ZmqSharp multipart.
        var reply = new NetMQMessage();
        reply.Append(new NetMQFrame([.. "x"u8]));
        reply.Append(new NetMQFrame([.. "y"u8]));
        peer.SendMultipartMessage(reply);

        var message = await ReadMessageAsync(socket.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(message);
        Assert.Equal(2, message.Value.Count);
        Assert.Equal([.. "x"u8], message.Value[0].ToSequence().ToArray());
        Assert.Equal([.. "y"u8], message.Value[1].ToSequence().ToArray());
        message.Value.Dispose();
    }

    [Fact(Timeout = 20_000)]
    public async Task LongFrame_BothDirections()
    {
        var payload = new byte[100_000];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 251);

        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // ZmqSharp -> NetMQ long frame (crosses many TCP segments).
        await socket.SendAsync(ZMessage.FromOwned(payload), token);
        var received = InteropHelpers.ReceiveFrame(peer, TimeSpan.FromSeconds(10));
        Assert.Equal(payload, received);

        // NetMQ -> ZmqSharp long frame.
        peer.SendFrame(payload);
        var message = await ReadMessageAsync(socket.Messages, TimeSpan.FromSeconds(10), token);
        Assert.NotNull(message);
        Assert.Equal(payload, message.Value[0].ToSequence().ToArray());
        message.Value.Dispose();
    }

    [Fact(Timeout = 20_000)]
    public async Task NetMQPeerClose_RaisesPeerEnded()
    {
        var peerEnded = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var peer = new PairSocket();
        peer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        peer.Bind($"tcp://127.0.0.1:{port}");

        await using var socket = new ZPairSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        socket.PeerEnded += (_, failure) => peerEnded.TrySetResult(failure);
        var token = TestContext.Current.CancellationToken;
        await socket.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // A graceful NetMQ close surfaces as a clean EOF on our side.
        peer.Close();

        var failure = await peerEnded.Task.WaitAsync(token);
        Assert.True(failure is null or IOException or SocketException);
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
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return null;
        }
    }
}
