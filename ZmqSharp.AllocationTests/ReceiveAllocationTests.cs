using System.Buffers;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Allocation measurements of the receive hot path, sampled on the receiving
/// pump thread itself. The old in-suite receive test read GC counters on the
/// test thread, which never sees the pump thread's allocations; these tests
/// capture the counter inside the semantic seam (0007 2.3) that the pump
/// thread executes synchronously. Each test runs in a dedicated xunit
/// collection (configured non-parallel), so no other test's allocations
/// pollute the process-wide counter.
///
/// The measured window is single-threaded by construction, not by luck: the
/// enqueue loop never awaits, so when the pump wakes the channel is already
/// fully buffered and every read completes synchronously - the pump cannot
/// migrate threads mid-window (the migration that follows the forced GC falls
/// on the warm-up boundary and is excluded). A thread-id invariant asserts
/// this, so the thread-local GC counter is used inside a provably one-thread
/// window rather than by assumption.
///
/// Measured reality: a pooled 1-byte frame costs a fixed ~216 bytes per
/// message on the pump thread (one MemoryPool rent + its owner wrapper per
/// frame - 0008 Pooled materialization). The assertions therefore bound the
/// per-message cost rather than demanding zero: they fail on leakage
/// (unbounded growth), regression to a larger per-message allocation, or a
/// counter that goes backwards on a single thread.
/// </summary>
[Collection("allocation-measurement")]
public class ReceiveAllocationTests
{
    private const int MessageCount = 2000;
    private const int WarmupCount = 64;

    [Fact(Timeout = 15_000)]
    public async Task Receive_SteadyState_PerMessageCostIsBoundedOnPumpThread()
    {
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket();
        socket.BindMessageSink(sink);
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));
        var peer = AllocationFakeTransport.Current!;

        // Warm up: the first deliveries pay one-time costs (pool size-class
        // caches, delegate caches, tiered JIT, scratch growth).
        for (var i = 0; i < WarmupCount; i++)
            peer.Enqueue(AllocationFrameData.Frame([(byte)i]));
        await sink.WaitForAsync(WarmupCount);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        for (var i = WarmupCount; i < MessageCount; i++)
            peer.Enqueue(AllocationFrameData.Frame([(byte)i]));
        await sink.WaitForAsync(MessageCount);

        // The measured window is single-threaded by construction (see the
        // class doc): WindowDeltas asserts the invariant instead of silently
        // dropping cross-thread samples.
        var deltas = WindowDeltas(sink);

        // The counter on one thread is monotonic while this test owns it.
        deltas.Min().Should().BeGreaterThanOrEqualTo(0);
#if !DEBUG
        // Bounded per-message cost: ~216 B for one pooled rent. A leak would
        // grow without bound; a regression to a large per-message allocation
        // blows this ceiling. Debug boxes async state machines per delivery,
        // so the absolute gates only hold in Release (0006 3.6); CI is Release.
        deltas.Max().Should().BeLessThanOrEqualTo(2048);
        // Median reflects the steady per-message pool cost, not a heavy tail.
        Median(deltas).Should().BeLessThanOrEqualTo(512);
#else
        _ = deltas;
#endif
    }

    [Fact(Timeout = 15_000)]
    public async Task Receive_EachMessage_RentsExactlyOnePooledBuffer()
    {
        using var pool = new CountingRentPool();
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket(new ZSocketOptions { Pool = pool });
        socket.BindMessageSink(sink);
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));
        var peer = AllocationFakeTransport.Current!;

        const int count = 1000;
        for (var i = 0; i < count; i++)
            peer.Enqueue(AllocationFrameData.Frame([(byte)i]));
        await sink.WaitForAsync(count);

        // One rent per single-frame message (0008 Pooled materialization);
        // the +2 covers the parser's initial scratch rent and the handshake.
        pool.Rentals.Should().Be(count + 2);
    }

    [Fact(Timeout = 15_000)]
    public async Task Receive_FirstDelivery_AllocatesThenSteadies()
    {
        var sink = new MeasuringSink(WarmupCount);
        await using var socket = new ZPairSocket();
        socket.BindMessageSink(sink);
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));
        var peer = AllocationFakeTransport.Current!;

        peer.Enqueue(AllocationFrameData.Frame([.. "first"u8]));
        await sink.WaitForAsync(1);

        // The very first delivery allocates (scratch rent + pool warm-up); the
        // ceiling is only a sanity bound. Steady-state cost is asserted above.
        sink.Samples[0].Should().BeGreaterThan(0);
        sink.Samples[0].Should().BeLessThan(1 << 20);
    }

    [Fact(Timeout = 15_000)]
    public async Task Receive_MultiFrameMessage_PerMessageCostIsBoundedOnPumpThread()
    {
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket();
        socket.BindMessageSink(sink);
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));
        var peer = AllocationFakeTransport.Current!;

        byte[] firstFrame = [.. "first-frame"u8];
        byte[] secondFrame = [.. "second-frame"u8];
        for (var i = 0; i < WarmupCount; i++)
        {
            peer.Enqueue(AllocationFrameData.Frame(firstFrame, true));
            peer.Enqueue(AllocationFrameData.Frame(secondFrame));
        }

        await sink.WaitForAsync(WarmupCount);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        for (var i = WarmupCount; i < MessageCount; i++)
        {
            peer.Enqueue(AllocationFrameData.Frame(firstFrame, true));
            peer.Enqueue(AllocationFrameData.Frame(secondFrame));
        }

        await sink.WaitForAsync(MessageCount);

        var deltas = WindowDeltas(sink);
        deltas.Min().Should().BeGreaterThanOrEqualTo(0);
#if !DEBUG
        // ~2 pooled rents per two-frame message; absolute gates are Release
        // only (see the steady-state test).
        deltas.Max().Should().BeLessThanOrEqualTo(2048);
        Median(deltas).Should().BeLessThanOrEqualTo(1024);
#else
        _ = deltas;
#endif
    }

    /// <summary>
    /// Deltas across the measured window with the single-thread invariant
    /// asserted, not filtered: the enqueue loop never awaits, so the pump
    /// drains the fully-buffered channel with synchronously completing reads
    /// and cannot migrate threads mid-window. A migration would make the
    /// thread-local GC counter meaningless, so it must fail the test loudly
    /// instead of silently skewing the deltas.
    /// </summary>
    private static long[] WindowDeltas(MeasuringSink sink)
    {
        var windowThreads = sink.ThreadIds[WarmupCount..].Distinct().ToArray();
        windowThreads.Should().ContainSingle(
            "the measured window must run on a single pump thread for the thread-local deltas to be valid");

        var deltas = new long[MessageCount - WarmupCount - 1];
        for (var i = WarmupCount + 1; i < MessageCount; i++)
            deltas[i - WarmupCount - 1] = sink.Samples[i] - sink.Samples[i - 1];

        return deltas;
    }

    private static long Median(long[] values)
    {
        Array.Sort(values);
        return values.Length == 0 ? 0 : values[values.Length / 2];
    }
}

/// <summary>Pool that counts Rent calls, to pin the per-message pool behavior.</summary>
internal sealed class CountingRentPool : MemoryPool<byte>
{
    private readonly MemoryPool<byte> inner = Shared;
    private int rented;

    public int Rentals => Volatile.Read(ref rented);

    public override int MaxBufferSize => inner.MaxBufferSize;

    public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
    {
        Interlocked.Increment(ref rented);
        return inner.Rent(minimumBufferSize);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) inner.Dispose();
    }
}
