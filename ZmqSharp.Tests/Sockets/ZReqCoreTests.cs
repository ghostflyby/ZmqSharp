using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests.Sockets;

public sealed class ZReqCoreTests
{
    [Fact(Timeout = 40_000)]
    public async Task Cancellation_WaitsForSendAndRetiresBeforeReopeningSlot()
    {
        var token = TestContext.Current.CancellationToken;
        var first = new ZPeer();
        var next = new ZPeer();
        using var cancellation = new CancellationTokenSource();
        using var pool = new CountingMemoryPool();
        var sendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ZPeer[] peers = [first];
        var core = new ZReqCore(() => peers, async (_, message, _) =>
        {
            sendStarted.TrySetResult();
            await releaseSend.Task;
        }, peer =>
        {
            peer.Should().BeSameAs(first);
            peers = [next];
            retired.TrySetResult();
        });
        var request = core.RequestAsync(ZMessage.FromPooled(pool.Rent(8)), cancellation.Token);
        await sendStarted.Task.WaitAsync(token);
        await cancellation.CancelAsync();
        await retired.Task.WaitAsync(token);
        request.IsCompleted.Should().BeFalse();
        pool.Outstanding.Should().Be(1);
        releaseSend.TrySetResult();
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<OperationCanceledException>();
        pool.Outstanding.Should().Be(0);

        var second = core.RequestAsync(ZMessage.Copy("second"u8.ToArray()), token);
        var late = ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8)));
        await core.DecideAsync(first, late, token);
        pool.Outstanding.Should().Be(0);
        second.IsCompleted.Should().BeFalse();
        await core.DecideAsync(next, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), token);
        using var reply = await second.WaitAsync(token);
        reply[0].ToSequence().ToArray().Should().Equal("reply"u8.ToArray());
    }

    [Fact(Timeout = 20_000)]
    public async Task MalformedReply_FaultsOriginalRequestAndReleasesBuffers()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        var core = CreateCore(peer);
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), token);
        await FluentActions.Awaiting(async () =>
                await core.DecideAsync(peer, ZMessage.FromPooled(pool.Rent(8)), token))
            .Should().ThrowAsync<ZeroMqProtocolException>();
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<ZeroMqProtocolException>();
        pool.Outstanding.Should().Be(0);
        var second = core.RequestAsync(ZMessage.Copy("next"u8.ToArray()), token);
        core.OnPeerEnded(peer);
        await FluentActions.Awaiting(() => second.WaitAsync(token)).Should().ThrowAsync<IOException>();
    }

    [Fact(Timeout = 10_000)]
    public async Task ReplyBeforeSendFinishes_DoesNotEndBorrowOrAllowAnotherRequest()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, message, _) => { await release.Task; }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("first"u8.ToArray()), token);
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), token);
        request.IsCompleted.Should().BeFalse();
        using var rejected = ZMessage.Copy("second"u8.ToArray());
        FluentActions.Invoking(() => { core.RequestAsync(rejected, token); }).Should().Throw<InvalidOperationException>();
        release.TrySetResult();
        using var reply = await request.WaitAsync(token);
    }

    [Fact(Timeout = 10_000)]
    public async Task CancellationAfterSend_CompletesRequestAndDisposesLosingReply()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        using var cancellation = new CancellationTokenSource();
        var retired = false;
        var core = CreateCore(peer, () => retired = true);
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), cancellation.Token);
        await cancellation.CancelAsync();
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<OperationCanceledException>();
        retired.Should().BeTrue();
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token);
        pool.Outstanding.Should().Be(0);
    }

    [Fact(Timeout = 10_000)]
    public async Task PeerEndsDuringSend_RequestWaitsForBufferRelease()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, message, _) => { await release.Task; }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), token);
        core.OnPeerEnded(peer);
        request.IsCompleted.Should().BeFalse();
        release.TrySetResult();
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<IOException>();
    }

    [Fact(Timeout = 10_000)]
    public async Task SendFailure_ReclaimsMessageRetiresPeerAndFaultsRequest()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        var retired = false;
        var core = new ZReqCore(() => [peer], (_, _, _) => throw new IOException("write failed"),
            _ => retired = true);
        var request = core.RequestAsync(ZMessage.FromPooled(pool.Rent(8)), token);
        await FluentActions.Awaiting(() => request.WaitAsync(token)).Should().ThrowAsync<IOException>();
        pool.Outstanding.Should().Be(0);
        retired.Should().BeTrue();
    }

    [Fact]
    public void PreCanceledRequest_DoesNotTakeOwnershipOrSend()
    {
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        using var message = ZMessage.FromPooled(pool.Rent(8));
        var core = new ZReqCore(() => [peer], (_, _, _) => throw new InvalidOperationException("unexpected send"),
            _ => throw new InvalidOperationException("unexpected retirement"));
        FluentActions.Invoking(() => { core.RequestAsync(message, new CancellationToken(true)); })
            .Should().Throw<OperationCanceledException>();
        pool.Outstanding.Should().Be(1);
    }

    [Fact(Timeout = 20_000)]
    public async Task ReplyAndCancellationRace_CompleteOnceAndReclaimLosingReply()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        for (var iteration = 0; iteration < 32; iteration++)
        {
            using var cancellation = new CancellationTokenSource();
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var retirements = 0;
            var core = CreateCore(peer, () => Interlocked.Increment(ref retirements));
            var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), cancellation.Token);
            var cancel = CancelAsync();
            var deliver = DeliverAsync();
            start.TrySetResult();
            await Task.WhenAll(cancel, deliver).WaitAsync(token);
            try
            {
                using var reply = await request.WaitAsync(token);
                reply.Should().HaveCount(1);
                retirements.Should().Be(0);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                retirements.Should().Be(1);
            }

            pool.Outstanding.Should().Be(0);

            async Task CancelAsync()
            {
                await start.Task;
                await cancellation.CancelAsync();
            }

            async Task DeliverAsync()
            {
                await start.Task;
                await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token);
            }
        }
    }

    private static ZReqCore CreateCore(ZPeer peer, Action? retired = null)
        => new(() => [peer], (_, message, _) => { return ValueTask.CompletedTask; }, _ => retired?.Invoke());
}
