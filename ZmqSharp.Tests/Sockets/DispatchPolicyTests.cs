using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Unit tests for the dispatch-policy seam (0015 section 2.1): each policy
/// selects the outbound connections without touching a socket. Round-robin
/// alternation and multi-target broadcast are asserted against fake
/// connections only; a custom multi-target policy is exercised end-to-end
/// through the base selective send path.
/// </summary>
public sealed class DispatchPolicyTests
{
    [Fact]
    public void RoundRobin_AlternatesAcrossPeers()
    {
        var policy = new ZRoundRobinDispatch();
        var a = new ZPeer();
        var b = new ZPeer();
        var c = new ZPeer();
        ZPeer[] peers = [a, b, c];
        var message = ZMessage.FromOwned([.. "x"u8]);

        var selections = new ZPeer?[6];
        for (var i = 0; i < selections.Length; i++)
            selections[i] = SelectOnly(policy, message, peers);

        selections.Should().Equal(a, b, c, a, b, c);
        message.Dispose();
    }

    [Fact]
    public void RoundRobin_NoPeers_DropsMessage()
    {
        var policy = new ZRoundRobinDispatch();
        var message = ZMessage.FromOwned([.. "x"u8]);

        policy.SelectTargets(message, ReadOnlySpan<ZPeer>.Empty, Span<ZPeer>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void RoundRobin_SinglePeer_AlwaysSelectsIt()
    {
        var policy = new ZRoundRobinDispatch();
        var peer = new ZPeer();
        var message = ZMessage.FromOwned([.. "x"u8]);

        for (var i = 0; i < 3; i++)
            SelectOnly(policy, message, [peer]).Should().BeSameAs(peer);

        message.Dispose();
    }

    [Fact]
    public void SinglePeer_ReturnsFirstConnection()
    {
        var policy = new ZSinglePeerDispatch();
        var first = new ZPeer();
        var second = new ZPeer();
        var message = ZMessage.FromOwned([.. "x"u8]);

        SelectOnly(policy, message, [first, second]).Should().BeSameAs(first);

        message.Dispose();
    }

    [Fact]
    public void SinglePeer_NoPeers_DropsMessage()
    {
        var policy = new ZSinglePeerDispatch();
        var message = ZMessage.FromOwned([.. "x"u8]);

        policy.SelectTargets(message, ReadOnlySpan<ZPeer>.Empty, Span<ZPeer>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void Broadcast_SelectsEveryPeer()
    {
        var policy = new ZBroadcastDispatch();
        var a = new ZPeer();
        var b = new ZPeer();
        ZPeer[] peers = [a, b];
        var message = ZMessage.FromOwned([.. "x"u8]);
        ZPeer[] targets = new ZPeer[peers.Length];

        var count = policy.SelectTargets(message, peers, targets);

        count.Should().Be(2);
        targets.AsSpan(0, count).ToArray().Should().Equal(a, b);
        message.Dispose();
    }

    [Fact]
    public void Broadcast_NoPeers_DropsMessage()
    {
        var policy = new ZBroadcastDispatch();
        var message = ZMessage.FromOwned([.. "x"u8]);

        policy.SelectTargets(message, ReadOnlySpan<ZPeer>.Empty, Span<ZPeer>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void Identity_GenericSendPath_Throws()
    {
        var policy = new ZIdentityDispatch();
        var act = () => SelectOnly(policy, ZMessage.FromOwned([.. "x"u8]), [new ZPeer()]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*SendAsync(identity, message)*");
    }

    [Fact]
    public void CurrentPeer_GenericSendPath_Throws()
    {
        var policy = new ZCurrentPeerDispatch();
        var act = () => SelectOnly(policy, ZMessage.FromOwned([.. "x"u8]), [new ZPeer()]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*RequestAsync*");
    }

    [Fact]
    public void CurrentPeer_WithInFlightRequest_SelectsTheCurrentConnection()
    {
        // REQ's request send routes through the policy: the current connection
        // recorded under the in-flight gate is the target SelectTargets returns.
        var policy = new ZCurrentPeerDispatch();
        var peer = new ZPeer();
        var message = ZMessage.FromOwned([.. "x"u8]);
        ZPeer[] targets = new ZPeer[1];

        policy.SetCurrent(peer);
        policy.SelectTargets(message, [peer], targets).Should().Be(1);
        targets[0].Should().BeSameAs(peer);

        policy.Clear();
        var act = () => policy.SelectTargets(message, [peer], targets);
        act.Should().Throw<InvalidOperationException>().WithMessage("*RequestAsync*");

        message.Dispose();
    }

    [Fact]
    public void IdentityDispatch_AssignsAndResolvesRoutingIds()
    {
        // ROUTER's identity routing table lives in the policy: inbound peers
        // are assigned their routing id here and directed sends resolve
        // through it; teardown releases the mapping.
        var policy = new ZIdentityDispatch();
        var peer = new ZPeer();

        var identity = policy.AssignIdentity(peer);
        identity.Should().NotBeEmpty();

        policy.TryResolve(identity, out var resolved).Should().BeTrue();
        resolved.Should().BeSameAs(peer);

        policy.RemovePeer(peer);
        policy.TryResolve(identity, out resolved).Should().BeFalse();
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CustomMultiSelectPolicy_DeliversToEverySelectedPeer(TransportKind kind)
    {
        // The policy is the primary decision maker on the selective send path:
        // a custom policy that selects every peer drives the base send, and
        // every selected peer receives the message exactly once. The route is
        // the policy's contract, not a socket override.
        var endpointA = TestTransports.GetEndpoint(kind);
        var endpointB = TestTransports.GetEndpoint(kind);
        await using var sender = new MultiSelectSocket();
        var receivedA = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receivedB = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var receiverA = new ZPairSocket(new ZSocketOptions { MessageSink = new TestSink(message => receivedA.TrySetResult(message)) });
        await using var receiverB = new ZPairSocket(new ZSocketOptions { MessageSink = new TestSink(message => receivedB.TrySetResult(message)) });
        var token = TestContext.Current.CancellationToken;

        await receiverA.BindAsync(endpointA, token);
        await receiverB.BindAsync(endpointB, token);
        await sender.ConnectAsync(endpointA, token);
        await sender.ConnectAsync(endpointB, token);

        // The custom policy selects both established peers.
        await sender.SendAsync(ZMessage.FromOwned([.. "both"u8]), token);
        (await receivedA.Task.WaitAsync(token))[0].ToSequence().ToArray().Should().Equal([.. "both"u8]);
        (await receivedB.Task.WaitAsync(token))[0].ToSequence().ToArray().Should().Equal([.. "both"u8]);
    }

    private static ZPeer? SelectOnly(IZDispatchPolicy policy, ZMessage message, ZPeer[] peers)
    {
        var targets = new ZPeer[1];
        return policy.SelectTargets(message, peers, targets) == 0 ? null : targets[0];
    }

    /// <summary>A policy that selects every established peer (a custom broadcast).</summary>
    private sealed class SelectAllDispatch : IZDispatchPolicy
    {
        public int SelectTargets(ZMessage message, ReadOnlySpan<ZPeer> peers, Span<ZPeer> targets)
        {
            peers.CopyTo(targets);
            return peers.Length;
        }
    }

    /// <summary>A test composition root with a pair-shaped socket type and a multi-select policy.</summary>
    private sealed class MultiSelectSocket() : ZSocketBase(new ZSocketOptions(), new SelectAllDispatch(), ZSocketTypes.Pair)
    {
        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            return SendAsyncCore(message, token);
        }
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}
