using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Interop;

/// <summary>
/// DEALER/ROUTER interop with the NetMQ libzmq-compatible implementation over
/// TCP (0006 section 5, 0012).
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class DealerRouterInteropTests
{
    [Fact]
    public async Task ZmqSharpDealer_NetMQDealer_BothDirections()
    {
        using var dealer = new DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        await using var ours = ZSocket.CreateDealer(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ours.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // Ours -> NetMQ.
        await ours.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);
        var received = InteropHelpers.ReceiveFrame(dealer, TimeSpan.FromSeconds(5));
        received.Should().Equal([.. "hello"u8]);

        // NetMQ -> ours.
        dealer.SendFrame([.. "world"u8]);
        var message = await ReadMessageAsync(ours.Messages, TimeSpan.FromSeconds(5));
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "world"u8]);
        message.Value.Dispose();
    }

    [Fact]
    public async Task NetMQDealer_ZmqSharpRouter_IdentityFraming()
    {
        // ZMTP 3.0 does not transmit identities over the wire: the router
        // assigns each peer its own routing id and prefixes it to inbound
        // messages; a reply re-framed with that id routes back to the peer.
        using var dealer = new DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        var routedMessage = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var router = ZSocket.CreateRouterCallback();
        router.BindMessageSink(new TestSink(message => routedMessage.TrySetResult(message)));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await router.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // The dealer sends [payload]; our router prefixes its own routing id.
        dealer.SendFrame([.. "ping"u8]);
        var routed = await routedMessage.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var identity = routed[0].ToSequence().ToArray();
        identity.Should().NotBeEmpty();
        routed[1].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        routed.Dispose();

        // Reply with the identity frame -> routes back to the dealer.
        await router.SendAsync(identity, ZMessage.FromOwned([.. "pong"u8]), cts.Token);
        var reply = InteropHelpers.ReceiveFrame(dealer, TimeSpan.FromSeconds(5));
        reply.Should().Equal([.. "pong"u8]);
    }

    private static async Task<ZMessage?> ReadMessageAsync(ChannelReader<ZMessage> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}
