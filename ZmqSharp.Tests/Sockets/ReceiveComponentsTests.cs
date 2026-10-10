using System.Buffers;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

public sealed class ReceiveComponentsTests
{
    [Fact(Timeout = 10_000)]
    public async Task WakeGate_CompletesCapturedSignalAndRearms()
    {
        var token = TestContext.Current.CancellationToken;
        var wake = new WakeGate();
        var first = wake.Capture();
        wake.Wake();
        await first.WaitAsync(token);
        Assert.False(wake.Capture().IsCompleted);
    }

    [Fact(Timeout = 10_000)]
    public async Task AggregateReader_CapturesWakeBeforeInspectingQueues()
    {
        var token = TestContext.Current.CancellationToken;
        var wake = new WakeGate();
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inspections = 0;
        // A producer signals during the level check, before the reader awaits.
        var reader = new AggregateReader(() =>
        {
            if (++inspections == 1) wake.Wake();
            return [];
        }, wake, finished.Task);
        Assert.True(await reader.WaitToReadAsync(token).AsTask());
        var waiting = reader.WaitToReadAsync(token).AsTask();
        Assert.False(waiting.IsCompleted);
        finished.TrySetResult();
        Assert.False(await waiting.WaitAsync(token));
    }

    [Fact]
    public async Task AggregateReader_StoppedRecordCannotTransferQueuedOwnership()
    {
        using var pool = new CountingMemoryPool();
        using var connection = new EstablishedFakeConnection();
        var registration = new ZEndpointRegistration(connection, null, null, null, CancellationToken.None, abort: connection.Abort);
        var record = new PeerRecord(connection, registration, false)
        {
            Phase = PeerPhase.Established,
            Queue = Channel.CreateUnbounded<ZMessage>()
        };
        var queue = record.Queue;
        Assert.True(queue.Writer.TryWrite(ZMessage.FromPooled(pool.Rent(8))));
        var reader = new AggregateReader(() => [record], new WakeGate(), Task.CompletedTask);
        record.Phase = PeerPhase.Stopping;
        Assert.False(reader.TryRead(out _));
        Assert.Equal(1, pool.Outstanding);
        lock (record.ReadLock)
        {
            Assert.True(queue.Reader.TryRead(out var held));
            held.Dispose();
            record.Queue = null;
        }

        Assert.Equal(0, pool.Outstanding);
        await registration.FinishAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task QueueReader_AndReclaim_TransferEachOwnerExactlyOnce()
    {
        var token = TestContext.Current.CancellationToken;
        await using var runtime = new SocketRuntime(new ZSocketOptions(),
            new ZSinglePeerDispatch(), ZSocketTypes.Pair, supportsQueue: true);
        var surface = runtime.QueueSurface ?? throw new InvalidOperationException();
        using var connection = new EstablishedFakeConnection();
        var registration = new ZEndpointRegistration(connection, null, null, null, CancellationToken.None, abort: connection.Abort);
        var record = new PeerRecord(connection, registration, false) { Phase = PeerPhase.Established };
        lock (runtime.StateLock) surface.Add(record);
        var owners = Enumerable.Range(0, 8).Select(_ => new OnceOwner()).ToArray();
        foreach (var owner in owners) await surface.DeliverAsync(record, ZMessage.FromPooled(owner), token);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reading = Task.Run(async () =>
        {
            await start.Task;
            while (surface.Messages.TryRead(out var message)) message.Dispose();
        }, token);
        var reclaiming = Task.Run(async () =>
        {
            await start.Task;
            lock (runtime.StateLock)
            {
                record.Phase = PeerPhase.Stopping;
                surface.Remove(record);
            }

            surface.Reclaim(record, null);
        }, token);
        start.TrySetResult();
        await Task.WhenAll(reading, reclaiming).WaitAsync(token);
        Assert.All(owners, owner => Assert.Equal(1, owner.Disposals));
        Assert.False(surface.Messages.TryRead(out _));
        await registration.FinishAsync();
    }

    private sealed class OnceOwner : IMemoryOwner<byte>
    {
        public Memory<byte> Memory { get; } = new byte[8];
        public int Disposals;

        public void Dispose()
        {
            if (Interlocked.Increment(ref Disposals) != 1) throw new InvalidOperationException("owner released twice");
        }
    }

    [Fact]
    public async Task Parser_ReadFailureAfterMaterialization_ReclaimsTheUntransferredFrame()
    {
        var token = TestContext.Current.CancellationToken;
        using var pool = new CountingMemoryPool();
        var materializer = new ReceiveMaterializer(pool, new ZReceiveOptions(), 100, 100, 10, () => { });
        using var parser = new ZmtpParser(new HeaderThenFailure(), (_, _) =>
            throw new InvalidOperationException("frame must not be delivered"), materializer.CreateAllocator(), pool);
        await Assert.ThrowsAsync<IOException>(() => parser.ParseAsync(token).AsTask());
        Assert.Equal(0, pool.Outstanding);
    }

    private sealed class HeaderThenFailure : IZByteReader
    {
        private int position;

        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        {
            if (position >= 2) throw new IOException("read failed in body");
            destination.Span[0] = position++ == 0 ? (byte)0 : (byte)8;
            return ValueTask.FromResult(1);
        }
    }

    [Fact]
    public void Materializer_RejectsBeforeAllocationAndResetsAtMessageBoundary()
    {
        using var pool = new CountingMemoryPool();
        var rejected = 0;
        var decisions = new List<ZReceiveContext>();
        var policy = new ZDelegateReceivePolicy(context =>
        {
            decisions.Add(context);
            return new ZReceiveAllocation { Mode = ZReceiveMode.Pooled };
        });
        var materializer = new ReceiveMaterializer(pool, policy, 8, 12, 2, () => rejected++);
        var allocate = materializer.CreateAllocator();
        using (var first = allocate(8, true)) Assert.Equal(8, first.ToSequence().Length);
        Assert.Throws<ZReceiveRejectedException>(() => allocate(8, false));
        Assert.Single(decisions);
        Assert.Equal(1, rejected);
        Assert.Equal(0, pool.Outstanding);
        materializer.Reset();
        using var next = allocate(4, false);
        Assert.Equal(0, decisions[1].FrameIndex);
        Assert.Equal(4, decisions[1].AccumulatedLength);
    }

    [Fact]
    public void Materializer_SegmentedFrameReleasesEveryOwnerOnce()
    {
        using var pool = new CountingMemoryPool();
        var materializer = new ReceiveMaterializer(pool,
            new ZReceiveOptions { ContiguousFrameLimit = 1 }, int.MaxValue, int.MaxValue, 10, () => { });
        var frame = materializer.CreateAllocator()(20_000, false);
        Assert.Equal(3, frame.Count);
        Assert.Equal(3, pool.Outstanding);
        frame.Dispose();
        Assert.Equal(0, pool.Outstanding);
    }
}
