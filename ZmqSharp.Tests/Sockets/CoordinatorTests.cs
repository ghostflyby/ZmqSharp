using System.Buffers;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests.Sockets;

public sealed class CoordinatorTests
{
    [Fact(Timeout = 10_000)]
    public async Task Rep_SerializesHandlersAcrossPeersAndRoutesReplies()
    {
        var token = TestContext.Current.CancellationToken;
        using var pool = new CountingMemoryPool();
        var first = new ZPeer();
        var second = new ZPeer();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new List<ZPeer>();
        var replies = new List<ZPeer>();
        ZRepCore? coordinator = null;
        var core = new ZRepCore(async (context, handlerToken) =>
        {
            handled.Add(context.Peer);
            if (ReferenceEquals(context.Peer, first))
            {
                entered.TrySetResult();
                await release.Task;
            }

            if (coordinator is not null)
                await coordinator.SendReplyAsync(context, ZMessage.Copy("reply"u8.ToArray()), handlerToken);
        }, (peer, message, _) =>
        {
            replies.Add(peer);
            using var reply = ZDelimiterFraming.Decode(message, "reply");
            Assert.Equal("reply"u8.ToArray(), reply[0].ToSequence().ToArray());
            return ValueTask.CompletedTask;
        });
        coordinator = core;
        var one = core.DecideAsync(first, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token).AsTask();
        await entered.Task.WaitAsync(token);
        var two = core.DecideAsync(second, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token).AsTask();
        Assert.Equal([first], handled);
        Assert.False(two.IsCompleted);
        release.TrySetResult();
        await Task.WhenAll(one, two).WaitAsync(token);
        Assert.Equal([first, second], handled);
        Assert.Equal([first, second], replies);
        Assert.Equal(0, pool.Outstanding);
    }

    [Fact]
    public async Task Rep_CancelledWaitReclaimsMessageWithoutStartingHandler()
    {
        var token = TestContext.Current.CancellationToken;
        using var pool = new CountingMemoryPool();
        using var cancellation = new CancellationTokenSource();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZRepCore(async (_, _) => await release.Task, (_, message, _) =>
        {
            message.Dispose();
            return ValueTask.CompletedTask;
        });
        var active = core.DecideAsync(new ZPeer(), ZDelimiterFraming.Encode(ZMessage.Copy("active"u8.ToArray())), token).AsTask();
        var waiting = core.DecideAsync(new ZPeer(), ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), cancellation.Token).AsTask();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(0, pool.Outstanding);
        release.TrySetResult();
        await active;
    }

    [Fact]
    public async Task XPub_ForwardsOwnedCopiesOnlyToOtherPeers()
    {
        var token = TestContext.Current.CancellationToken;
        var source = new ZPeer();
        var other = new ZPeer();
        var forwarded = new List<ZMessage>();
        var core = new XPubCoordinator(() => [source, other], (peer, message) =>
        {
            Assert.Same(other, peer);
            forwarded.Add(message);
        });
        byte[] subscription = [1, 65];
        using var incoming = ZMessage.FromOwned(subscription);
        var decision = await core.DecideAsync(source, incoming, token);
        Assert.Equal(ZInboundAction.Deliver, decision.Action);
        subscription[1] = 66;
        using var copy = forwarded.Single();
        Assert.Equal([1, 65], copy[0].ToSequence().ToArray());
    }
}
