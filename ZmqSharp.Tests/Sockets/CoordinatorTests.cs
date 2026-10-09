using System.Buffers;
using FluentAssertions;
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
        var core = new ZRepCore(async (context, token) =>
        {
            handled.Add(context.Peer);
            if (ReferenceEquals(context.Peer, first))
            {
                entered.TrySetResult();
                await release.Task;
            }

            if (coordinator is { } replyCore)
                await replyCore.SendReplyAsync(context, ZMessage.Copy("reply"u8.ToArray()), token);
        }, (peer, message, _) =>
        {
            replies.Add(peer);
            using var reply = ZDelimiterFraming.Decode(message, "reply");
            reply[0].ToSequence().ToArray().Should().Equal("reply"u8.ToArray());
            return ValueTask.CompletedTask;
        });
        coordinator = core;
        var one = core.DecideAsync(first, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token).AsTask();
        await entered.Task.WaitAsync(token);
        var two = core.DecideAsync(second, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token).AsTask();
        handled.Should().Equal(first);
        two.IsCompleted.Should().BeFalse();
        release.TrySetResult();
        await Task.WhenAll(one, two).WaitAsync(token);
        handled.Should().Equal(first, second);
        replies.Should().Equal(first, second);
        pool.Outstanding.Should().Be(0);
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
        await FluentActions.Awaiting(() => waiting).Should().ThrowAsync<OperationCanceledException>();
        pool.Outstanding.Should().Be(0);
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
            peer.Should().BeSameAs(other);
            forwarded.Add(message);
        });
        byte[] subscription = [1, 65];
        using var incoming = ZMessage.FromOwned(subscription);
        var decision = await core.DecideAsync(source, incoming, token);
        decision.Action.Should().Be(ZInboundAction.Deliver);
        subscription[1] = 66;
        using var copy = forwarded.Single();
        copy[0].ToSequence().ToArray().Should().Equal(1, 65);
    }
}
