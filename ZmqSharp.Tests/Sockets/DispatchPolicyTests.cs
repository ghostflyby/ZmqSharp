using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

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
        var a = new FakeConnection();
        var b = new FakeConnection();
        var c = new FakeConnection();
        IZConnection[] peers = [a, b, c];
        var message = ZMessage.FromOwned([.. "x"u8]);

        var selections = new IZConnection?[6];
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

        policy.SelectTargets(message, ReadOnlySpan<IZConnection>.Empty, Span<IZConnection>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void RoundRobin_SinglePeer_AlwaysSelectsIt()
    {
        var policy = new ZRoundRobinDispatch();
        var peer = new FakeConnection();
        var message = ZMessage.FromOwned([.. "x"u8]);

        for (var i = 0; i < 3; i++)
            SelectOnly(policy, message, [peer]).Should().BeSameAs(peer);

        message.Dispose();
    }

    [Fact]
    public void SinglePeer_ReturnsFirstConnection()
    {
        var policy = new ZSinglePeerDispatch();
        var first = new FakeConnection();
        var second = new FakeConnection();
        var message = ZMessage.FromOwned([.. "x"u8]);

        SelectOnly(policy, message, [first, second]).Should().BeSameAs(first);

        message.Dispose();
    }

    [Fact]
    public void SinglePeer_NoPeers_DropsMessage()
    {
        var policy = new ZSinglePeerDispatch();
        var message = ZMessage.FromOwned([.. "x"u8]);

        policy.SelectTargets(message, ReadOnlySpan<IZConnection>.Empty, Span<IZConnection>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void Broadcast_SelectsEveryPeer()
    {
        var policy = new ZBroadcastDispatch();
        var a = new FakeConnection();
        var b = new FakeConnection();
        IZConnection[] peers = [a, b];
        var message = ZMessage.FromOwned([.. "x"u8]);
        IZConnection[] targets = new IZConnection[peers.Length];

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

        policy.SelectTargets(message, ReadOnlySpan<IZConnection>.Empty, Span<IZConnection>.Empty).Should().Be(0);

        message.Dispose();
    }

    [Fact]
    public void Identity_GenericSendPath_Throws()
    {
        var policy = new ZIdentityDispatch();
        var act = () => SelectOnly(policy, ZMessage.FromOwned([.. "x"u8]), [new FakeConnection()]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*SendAsync(identity, message)*");
    }

    [Fact]
    public void CurrentPeer_GenericSendPath_Throws()
    {
        var policy = new ZCurrentPeerDispatch();
        var act = () => SelectOnly(policy, ZMessage.FromOwned([.. "x"u8]), [new FakeConnection()]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*RequestAsync*");
    }

    [Fact]
    public void CurrentPeer_WithInFlightRequest_SelectsTheCurrentConnection()
    {
        // REQ's request send routes through the policy: the current connection
        // recorded under the in-flight gate is the target SelectTargets returns.
        var policy = new ZCurrentPeerDispatch();
        var peer = new FakeConnection();
        var message = ZMessage.FromOwned([.. "x"u8]);
        IZConnection[] targets = new IZConnection[1];

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
        var peer = new FakeConnection();

        var identity = policy.AssignIdentity(peer);
        identity.Should().NotBeEmpty();

        policy.TryResolve(identity, out var resolved).Should().BeTrue();
        resolved.Should().BeSameAs(peer);

        policy.RemovePeer(peer);
        policy.TryResolve(identity, out resolved).Should().BeFalse();
    }

    [Theory]
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
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await receiverA.BindAsync(endpointA, cts.Token);
        await receiverB.BindAsync(endpointB, cts.Token);
        await sender.ConnectAsync(endpointA, cts.Token);
        await sender.ConnectAsync(endpointB, cts.Token);

        // The custom policy selects both established peers.
        await sender.SendAsync(ZMessage.FromOwned([.. "both"u8]), cts.Token);
        (await receivedA.Task.WaitAsync(cts.Token))[0].ToSequence().ToArray().Should().Equal([.. "both"u8]);
        (await receivedB.Task.WaitAsync(cts.Token))[0].ToSequence().ToArray().Should().Equal([.. "both"u8]);
    }

    private static IZConnection? SelectOnly(IZDispatchPolicy policy, ZMessage message, IZConnection[] peers)
    {
        var targets = new IZConnection[1];
        return policy.SelectTargets(message, peers, targets) == 0 ? null : targets[0];
    }

    /// <summary>A policy that selects every established peer (a custom broadcast).</summary>
    private sealed class SelectAllDispatch : IZDispatchPolicy
    {
        public int SelectTargets(ZMessage message, ReadOnlySpan<IZConnection> peers, Span<IZConnection> targets)
        {
            peers.CopyTo(targets);
            return peers.Length;
        }
    }

    /// <summary>A test composition root with a pair-shaped socket type and a multi-select policy.</summary>
    private sealed class MultiSelectSocket : ZSocketBase
    {
        public MultiSelectSocket()
            : base(new ZSocketOptions(), new SelectAllDispatch(), ZSocketTypes.Pair)
        {
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

    private sealed class FakeConnection : IZConnection
    {
        // Dispatch policies never touch the connection; the contract members
        // are unreachable from SelectTargets.
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
            => throw new NotSupportedException();

        public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
            => throw new NotSupportedException();

        public void SetConnectionEndedHandler(Action onConnectionEnded)
            => throw new NotSupportedException();

        public void OnConnectionEnded()
            => throw new NotSupportedException();

        public void Dispose()
            => throw new NotSupportedException();
    }
}
