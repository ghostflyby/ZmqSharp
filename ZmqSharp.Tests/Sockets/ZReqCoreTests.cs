using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

public sealed class ZReqCoreTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Cancellation_WaitsForSendAndRetiresBeforeReopeningSlot()
    {
        using var first = new EstablishedFakeConnection();
        using var next = new EstablishedFakeConnection();
        using var cancellation = new CancellationTokenSource();
        using var pool = new CountingMemoryPool();
        var sendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        IZConnection[] peers = [first];
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
        await sendStarted.Task.WaitAsync(Timeout);
        await cancellation.CancelAsync();
        await retired.Task.WaitAsync(Timeout);
        request.IsCompleted.Should().BeFalse();
        pool.Outstanding.Should().Be(1);
        releaseSend.TrySetResult();
        await FluentActions.Awaiting(() => request.WaitAsync(Timeout)).Should().ThrowAsync<OperationCanceledException>();
        pool.Outstanding.Should().Be(0);

        var second = core.RequestAsync(ZMessage.Copy("second"u8.ToArray()), default);
        var late = ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8)));
        await core.DecideAsync(first, late, default);
        pool.Outstanding.Should().Be(0);
        second.IsCompleted.Should().BeFalse();
        await core.DecideAsync(next, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), default);
        using var reply = await second.WaitAsync(Timeout);
        reply[0].ToSequence().ToArray().Should().Equal("reply"u8.ToArray());
    }

    [Fact]
    public async Task MalformedReply_FaultsOriginalRequestAndReleasesBuffers()
    {
        using var peer = new EstablishedFakeConnection();
        using var pool = new CountingMemoryPool();
        var core = CreateCore(peer);
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), default);
        await FluentActions.Awaiting(async () =>
                await core.DecideAsync(peer, ZMessage.FromPooled(pool.Rent(8)), default))
            .Should().ThrowAsync<ZeroMqProtocolException>();
        await FluentActions.Awaiting(() => request.WaitAsync(Timeout)).Should().ThrowAsync<ZeroMqProtocolException>();
        pool.Outstanding.Should().Be(0);
        var second = core.RequestAsync(ZMessage.Copy("next"u8.ToArray()), default);
        core.OnPeerEnded(peer);
        await FluentActions.Awaiting(() => second.WaitAsync(Timeout)).Should().ThrowAsync<IOException>();
    }

    [Fact]
    public async Task ReplyBeforeSendFinishes_DoesNotEndBorrowOrAllowAnotherRequest()
    {
        using var peer = new EstablishedFakeConnection();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, message, _) =>
        {
            await release.Task;
        }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("first"u8.ToArray()), default);
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), default);
        request.IsCompleted.Should().BeFalse();
        using var rejected = ZMessage.Copy("second"u8.ToArray());
        FluentActions.Invoking(() => { core.RequestAsync(rejected, default); }).Should().Throw<InvalidOperationException>();
        release.TrySetResult();
        using var reply = await request.WaitAsync(Timeout);
    }

    [Fact]
    public async Task CancellationAfterSend_CompletesRequestAndDisposesLosingReply()
    {
        using var peer = new EstablishedFakeConnection();
        using var pool = new CountingMemoryPool();
        using var cancellation = new CancellationTokenSource();
        var retired = false;
        var core = CreateCore(peer, () => retired = true);
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), cancellation.Token);
        await cancellation.CancelAsync();
        await FluentActions.Awaiting(() => request.WaitAsync(Timeout)).Should().ThrowAsync<OperationCanceledException>();
        retired.Should().BeTrue();
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), default);
        pool.Outstanding.Should().Be(0);
    }

    [Fact]
    public async Task PeerEndsDuringSend_RequestWaitsForBufferRelease()
    {
        using var peer = new EstablishedFakeConnection();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, message, _) =>
        {
            await release.Task;
        }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), default);
        core.OnPeerEnded(peer);
        request.IsCompleted.Should().BeFalse();
        release.TrySetResult();
        await FluentActions.Awaiting(() => request.WaitAsync(Timeout)).Should().ThrowAsync<IOException>();
    }

    [Fact]
    public async Task SendFailure_ReclaimsMessageRetiresPeerAndFaultsRequest()
    {
        using var peer = new EstablishedFakeConnection();
        using var pool = new CountingMemoryPool();
        var retired = false;
        var core = new ZReqCore(() => [peer], (_, _, _) => throw new IOException("write failed"),
            _ => retired = true);
        var request = core.RequestAsync(ZMessage.FromPooled(pool.Rent(8)), default);
        await FluentActions.Awaiting(() => request.WaitAsync(Timeout)).Should().ThrowAsync<IOException>();
        pool.Outstanding.Should().Be(0);
        retired.Should().BeTrue();
    }

    [Fact]
    public void PreCanceledRequest_DoesNotTakeOwnershipOrSend()
    {
        using var peer = new EstablishedFakeConnection();
        using var pool = new CountingMemoryPool();
        using var message = ZMessage.FromPooled(pool.Rent(8));
        var core = new ZReqCore(() => [peer], (_, _, _) => throw new InvalidOperationException("unexpected send"),
            _ => throw new InvalidOperationException("unexpected retirement"));
        FluentActions.Invoking(() => { core.RequestAsync(message, new CancellationToken(true)); })
            .Should().Throw<OperationCanceledException>();
        pool.Outstanding.Should().Be(1);
    }

    [Fact]
    public async Task ReplyAndCancellationRace_CompleteOnceAndReclaimLosingReply()
    {
        using var peer = new EstablishedFakeConnection();
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
            await Task.WhenAll(cancel, deliver).WaitAsync(Timeout);
            try
            {
                using var reply = await request.WaitAsync(Timeout);
                reply.Should().HaveCount(1);
                retirements.Should().Be(0);
            }
            catch (OperationCanceledException)
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
                await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), default);
            }
        }
    }

    private static ZReqCore CreateCore(IZConnection peer, Action? retired = null)
        => new(() => [peer], (_, message, _) =>
        {
            return ValueTask.CompletedTask;
        }, _ => retired?.Invoke());
}
