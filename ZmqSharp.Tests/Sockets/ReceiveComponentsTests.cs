using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

public sealed class ReceiveComponentsTests
{
    [Fact]
    public async Task WakeGate_CompletesCapturedSignalAndRearms()
    {
        var wake = new WakeGate();
        var first = wake.Capture();
        wake.Wake();
        await first.WaitAsync(TimeSpan.FromSeconds(5));
        wake.Capture().IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task AggregateReader_CapturesWakeBeforeInspectingQueues()
    {
        var wake = new WakeGate();
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inspections = 0;
        // A producer signals during the level check, before the reader awaits.
        var reader = new AggregateReader(() =>
        {
            if (++inspections == 1) wake.Wake();
            return [];
        }, wake, finished.Task);
        (await reader.WaitToReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5))).Should().BeTrue();
        var waiting = reader.WaitToReadAsync().AsTask();
        waiting.IsCompleted.Should().BeFalse();
        finished.TrySetResult();
        (await waiting.WaitAsync(TimeSpan.FromSeconds(5))).Should().BeFalse();
    }

    [Fact]
    public async Task AggregateReader_StoppedRecordCannotTransferQueuedOwnership()
    {
        using var pool = new CountingMemoryPool();
        using var connection = new EstablishedFakeConnection();
        var registration = new ZEndpointRegistration(connection, null, null, null, default, abort: connection.Abort);
        var record = new PeerRecord(connection, registration, false)
        {
            Phase = PeerPhase.Established,
            Queue = Channel.CreateUnbounded<ZMessage>()
        };
        var queue = record.Queue;
        queue.Writer.TryWrite(ZMessage.FromPooled(pool.Rent(8))).Should().BeTrue();
        var reader = new AggregateReader(() => [record], new WakeGate(), Task.CompletedTask);
        record.Phase = PeerPhase.Stopping;
        reader.TryRead(out _).Should().BeFalse();
        pool.Outstanding.Should().Be(1);
        lock (record.ReadLock)
        {
            queue.Reader.TryRead(out var held).Should().BeTrue();
            held.Dispose();
            record.Queue = null;
        }

        pool.Outstanding.Should().Be(0);
        await registration.FinishAsync();
    }

    [Fact]
    public async Task QueueReader_AndReclaim_TransferEachOwnerExactlyOnce()
    {
        await using var runtime = new SocketRuntime(new ZSocketOptions(),
            new ZSinglePeerDispatch(), ZSocketTypes.Pair, supportsQueue: true);
        var surface = runtime.QueueSurface ?? throw new InvalidOperationException();
        using var connection = new EstablishedFakeConnection();
        var registration = new ZEndpointRegistration(connection, null, null, null, default, abort: connection.Abort);
        var record = new PeerRecord(connection, registration, false) { Phase = PeerPhase.Established };
        lock (runtime.StateLock) surface.Add(record);
        var owners = Enumerable.Range(0, 8).Select(_ => new OnceOwner()).ToArray();
        foreach (var owner in owners) await surface.DeliverAsync(record, ZMessage.FromPooled(owner), default);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reading = Task.Run(async () =>
        {
            await start.Task;
            while (surface.Messages.TryRead(out var message)) message.Dispose();
        });
        var reclaiming = Task.Run(async () =>
        {
            await start.Task;
            lock (runtime.StateLock)
            {
                record.Phase = PeerPhase.Stopping;
                surface.Remove(record);
            }

            surface.Reclaim(record, null);
        });
        start.TrySetResult();
        await Task.WhenAll(reading, reclaiming).WaitAsync(TimeSpan.FromSeconds(5));
        owners.Should().OnlyContain(owner => owner.Disposals == 1);
        surface.Messages.TryRead(out _).Should().BeFalse();
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
        using var pool = new CountingMemoryPool();
        var materializer = new ReceiveMaterializer(pool, new ZReceiveOptions(), 100, 100, 10, () => { });
        using var parser = new ZmtpParser(new HeaderThenFailure(), (_, _) =>
            throw new InvalidOperationException("frame must not be delivered"), materializer.CreateAllocator(), pool);
        await FluentActions.Awaiting(() => parser.ParseAsync().AsTask()).Should().ThrowAsync<IOException>();
        pool.Outstanding.Should().Be(0);
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
        using (var first = allocate(8, true)) first.ToSequence().Length.Should().Be(8);
        FluentActions.Invoking(() => allocate(8, false)).Should().Throw<ZReceiveRejectedException>();
        decisions.Should().HaveCount(1);
        rejected.Should().Be(1);
        pool.Outstanding.Should().Be(0);
        materializer.Reset();
        using var next = allocate(4, false);
        decisions[1].FrameIndex.Should().Be(0);
        decisions[1].AccumulatedLength.Should().Be(4);
    }

    [Fact]
    public void Materializer_SegmentedFrameReleasesEveryOwnerOnce()
    {
        using var pool = new CountingMemoryPool();
        var materializer = new ReceiveMaterializer(pool,
            new ZReceiveOptions { ContiguousFrameLimit = 1 }, int.MaxValue, int.MaxValue, 10, () => { });
        var frame = materializer.CreateAllocator()(20_000, false);
        frame.Count.Should().Be(3);
        pool.Outstanding.Should().Be(3);
        frame.Dispose();
        pool.Outstanding.Should().Be(0);
    }
}
