using System.Buffers;
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
        var core = new ZReqCore(() => peers, async (_, _, _) =>
        {
            sendStarted.TrySetResult();
            await releaseSend.Task;
        }, peer =>
        {
            Assert.Same(first, peer);
            peers = [next];
            retired.TrySetResult();
        });
        var request = core.RequestAsync(ZMessage.FromPooled(pool.Rent(8)), cancellation.Token);
        await sendStarted.Task.WaitAsync(token);
        await cancellation.CancelAsync();
        await retired.Task.WaitAsync(token);
        Assert.False(request.IsCompleted);
        Assert.Equal(1, pool.Outstanding);
        releaseSend.TrySetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request.WaitAsync(token));
        Assert.Equal(0, pool.Outstanding);

        var second = core.RequestAsync(ZMessage.Copy("second"u8.ToArray()), token);
        var late = ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8)));
        await core.DecideAsync(first, late, token);
        Assert.Equal(0, pool.Outstanding);
        Assert.False(second.IsCompleted);
        await core.DecideAsync(next, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), token);
        using var reply = await second.WaitAsync(token);
        Assert.Equal("reply"u8.ToArray(), reply[0].ToSequence().ToArray());
    }

    [Fact(Timeout = 20_000)]
    public async Task MalformedReply_FaultsOriginalRequestAndReleasesBuffers()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        var core = CreateCore(peer);
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), token);
        await Assert.ThrowsAsync<ZeroMqProtocolException>(async () =>
            await core.DecideAsync(peer, ZMessage.FromPooled(pool.Rent(8)), token));
        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => request.WaitAsync(token));
        Assert.Equal(0, pool.Outstanding);
        var second = core.RequestAsync(ZMessage.Copy("next"u8.ToArray()), token);
        core.OnPeerEnded(peer);
        await Assert.ThrowsAsync<IOException>(() => second.WaitAsync(token));
    }

    [Fact(Timeout = 10_000)]
    public async Task ReplyBeforeSendFinishes_DoesNotEndBorrowOrAllowAnotherRequest()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, _, _) => { await release.Task; }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("first"u8.ToArray()), token);
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.Copy("reply"u8.ToArray())), token);
        Assert.False(request.IsCompleted);
        using var rejected = ZMessage.Copy("second"u8.ToArray());
        await Assert.ThrowsAsync<InvalidOperationException>(() => core.RequestAsync(rejected, token));
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
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request.WaitAsync(token));
        Assert.True(retired);
        await core.DecideAsync(peer, ZDelimiterFraming.Encode(ZMessage.FromPooled(pool.Rent(8))), token);
        Assert.Equal(0, pool.Outstanding);
    }

    [Fact(Timeout = 10_000)]
    public async Task PeerEndsDuringSend_RequestWaitsForBufferRelease()
    {
        var token = TestContext.Current.CancellationToken;
        var peer = new ZPeer();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var core = new ZReqCore(() => [peer], async (_, _, _) => { await release.Task; }, _ => { });
        var request = core.RequestAsync(ZMessage.Copy("request"u8.ToArray()), token);
        core.OnPeerEnded(peer);
        Assert.False(request.IsCompleted);
        release.TrySetResult();
        await Assert.ThrowsAsync<IOException>(() => request.WaitAsync(token));
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
        await Assert.ThrowsAsync<IOException>(() => request.WaitAsync(token));
        Assert.Equal(0, pool.Outstanding);
        Assert.True(retired);
    }

    [Fact]
    public async Task PreCanceledRequest_DoesNotTakeOwnershipOrSend()
    {
        var peer = new ZPeer();
        using var pool = new CountingMemoryPool();
        using var message = ZMessage.FromPooled(pool.Rent(8));
        var core = new ZReqCore(() => [peer], (_, _, _) => throw new InvalidOperationException("unexpected send"),
            _ => throw new InvalidOperationException("unexpected retirement"));
        await Assert.ThrowsAsync<OperationCanceledException>(() => core.RequestAsync(message, new CancellationToken(true)));
        Assert.Equal(1, pool.Outstanding);
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
                Assert.Single(reply);
                Assert.Equal(0, retirements);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                Assert.Equal(1, retirements);
            }

            Assert.Equal(0, pool.Outstanding);

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
        => new(() => [peer], (_, _, _) => { return ValueTask.CompletedTask; }, _ => retired?.Invoke());
}
