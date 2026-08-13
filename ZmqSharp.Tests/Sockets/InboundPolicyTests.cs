using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Inbound-policy seam tests (0019 section 3): the three-state decision
/// (Deliver / Drop / Consumed) and the composition surface. Unit tests cover
/// the ready-made policies; end-to-end tests exercise a custom inbound
/// transformation and a consume-style socket over TCP.
/// </summary>
public sealed class InboundPolicyTests
{
    [Fact]
    public async Task PassThrough_DeliversUnchanged()
    {
        var message = ZMessage.FromOwned([.. "x"u8]);

        var decision = await ZInboundPolicy.PassThrough.DecideAsync(new FakeConnection(), message, CancellationToken.None);

        decision.Action.Should().Be(ZInboundAction.Deliver);
        decision.Message.Should().BeNull();
        message.Dispose();
    }

    [Fact]
    public async Task Delegate_Drop_DisposesTheMessage()
    {
        var message = ZMessage.FromOwned([.. "x"u8]);
        var policy = new ZDelegateInboundPolicy((_, incoming, _) =>
        {
            incoming.Dispose();
            return ValueTask.FromResult(new ZInboundDecision { Action = ZInboundAction.Drop });
        });

        var decision = await policy.DecideAsync(new FakeConnection(), message, CancellationToken.None);

        decision.Action.Should().Be(ZInboundAction.Drop);
    }

    [Fact]
    public async Task Delegate_Consumed_OwnsTheMessage()
    {
        var message = ZMessage.FromOwned([.. "x"u8]);
        var policy = new ZDelegateInboundPolicy((_, incoming, _) =>
        {
            incoming.Dispose();
            return ValueTask.FromResult(new ZInboundDecision { Action = ZInboundAction.Consumed });
        });

        var decision = await policy.DecideAsync(new FakeConnection(), message, CancellationToken.None);

        decision.Action.Should().Be(ZInboundAction.Consumed);
    }

    [Fact]
    public void ForCustom_AcceptsOnlyTheSameName()
    {
        var type = ZSocketType.ForCustom("FOO");

        type.Name.Should().Be("FOO");
        type.AcceptsPeer("FOO").Should().BeTrue();
        type.AcceptsPeer("PAIR").Should().BeFalse();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CustomTransformInbound_DeliversReplacedMessage(TransportKind kind)
    {
        // A custom inbound policy drives the aggregated tier: the delivered
        // message is the policy's replacement (frames moved, 0019 section 3).
        // The socket reuses the PAIR identity, so a built-in pair connects.
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var server = new CustomTransformSocket();
        await using var client = ZSocket.CreatePairCallback();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        server.BindMessageSink(new TestSink(message => received.TrySetResult(message)));

        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);
        await client.SendAsync(ZMessage.FromOwned([.. "ping"u8]), cts.Token);

        var message = await received.Task.WaitAsync(cts.Token);
        message.Count.Should().Be(2);
        byte[] prefix = [.. "!"u8];
        byte[] payload = [.. "ping"u8];
        message[0].ToSequence().ToArray().Should().Equal(prefix);
        message[1].ToSequence().ToArray().Should().Equal(payload);
        message.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CustomConsumeInbound_ConsumesWithoutASink(TransportKind kind)
    {
        // A consume-style socket (custom REQ-like) aggregates without a bound
        // sink (0019 section 4): every message is consumed by the policy and
        // the peer's pump stays alive.
        var endpoint = TestTransports.GetEndpoint(kind);
        var inbound = new ConsumeInbound();
        await using var server = new ConsumeSocket(inbound);
        await using var client = ZSocket.CreatePairCallback();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);
        await client.SendAsync(ZMessage.FromOwned([.. "a"u8]), cts.Token);
        await client.SendAsync(ZMessage.FromOwned([.. "b"u8]), cts.Token);

        await WaitUntilAsync(() => inbound.Count, value => value == 2, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OnFrame_OnSocketWithInboundPolicy_Throws()
    {
        // A non-default inbound policy consumes the aggregated delivery
        // stream, so the raw frame surface must fail loudly instead of
        // silently delivering nothing (subagent review finding).
        await using var router = ZSocket.CreateRouterCallback();
        var act = () => router.OnFrame += (_, _) => true;

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*composed inbound policy*");
    }

    [Fact]
    public async Task OnFrame_OnPassThroughSocket_Subscribes()
    {
        await using var pair = ZSocket.CreatePairCallback();
        var subscribed = false;
        pair.OnFrame += (_, _) =>
        {
            subscribed = true;
            return true;
        };

        subscribed.Should().BeFalse();
    }

    /// <summary>Delivers every message prefixed with "!" (frames moved, 0007 M3).</summary>
    private sealed class PrefixInbound : IZInboundPolicy
    {
        public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            var frames = new List<ZFrame>(message.Count + 1)
            {
                new(new ZSegment((byte[])[.. "!"u8], 0, 1))
            };
            for (var i = 0; i < message.Count; i++) frames.Add(message[i]);

            return ValueTask.FromResult(new ZInboundDecision
            {
                Action = ZInboundAction.Deliver,
                Message = new ZMessage(new ZMultiMessage([.. frames]))
            });
        }
    }

    /// <summary>Counts and disposes every message (a custom REQ-like consume).</summary>
    private sealed class ConsumeInbound : IZInboundPolicy
    {
        private int count;

        public int Count => count;

        public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        {
            Interlocked.Increment(ref count);
            message.Dispose();
            return ValueTask.FromResult(new ZInboundDecision { Action = ZInboundAction.Consumed });
        }
    }

    private sealed class CustomTransformSocket(ZSocketOptions? options = null)
        : ZSocketBase(options ?? new ZSocketOptions(), new ZSinglePeerDispatch(),
            ZSocketTypes.Pair, new PrefixInbound());

    private sealed class ConsumeSocket(ConsumeInbound inbound, ZSocketOptions? options = null)
        : ZSocketBase(options ?? new ZSocketOptions(), new ZSinglePeerDispatch(),
            ZSocketTypes.Pair, inbound);

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
        // The inbound policies here never touch the connection; the contract
        // members are unreachable from DecideAsync.
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

    private static async Task WaitUntilAsync<T>(Func<T> getValue, Func<T, bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition(getValue()))
        {
            await Task.Delay(10, cts.Token);
            cts.Token.ThrowIfCancellationRequested();
        }
    }
}
