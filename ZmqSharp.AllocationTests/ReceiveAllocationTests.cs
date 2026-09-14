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
/// The measured window is single-threaded by construction: the fake
/// connection parks the pump at an empty channel, the test then enqueues
/// every measured frame and releases the pump, which drains the fully
/// buffered channel in one continuation - so the pump cannot migrate threads
/// mid-window. A thread-id invariant asserts this, so the thread-local GC
/// counter is used inside a provably one-thread window rather than by
/// assumption.
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

    /// <summary>Steady per-message ceiling on the pump thread; one pooled rent measures ~216 B.</summary>
    private const long PerMessageCeiling = 2048;

    /// <summary>
    /// How many one-off runtime allocations may land in a single window. See
    /// <see cref="AssertPerMessageCostBounded"/> for the measured evidence.
    /// </summary>
    private const int RuntimeEventAllowance = 2;

    /// <summary>
    /// Above this a single delta is a real regression rather than a runtime
    /// event: the one-off events measured here are ~16 KiB.
    /// </summary>
    private const long RuntimeEventCeiling = 1 << 16;

    [Fact(Timeout = 15_000)]
    public async Task Receive_SteadyState_PerMessageCostIsBoundedOnPumpThread()
    {
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket(new ZSocketOptions { MessageSink = sink });
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

        // Deterministic window: arm the fake's pump gate, wake the pump out of
        // its post-warmup channel block with one sentinel frame, and wait until
        // it parks at the gate. Every measured frame is then buffered while the
        // pump cannot consume, and the release lets it drain the fully buffered
        // channel in one continuation on one thread - so the thread-local GC
        // window below is single-threaded by construction, not by racing the
        // pump (a pump that parks mid-window would migrate to another
        // thread-pool thread and invalidate the deltas).
        peer.ArmSynchronousWindow();
        peer.Enqueue(AllocationFrameData.Frame([0x00]));
        await peer.WaitUntilPumpIdleAsync();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        for (var i = WarmupCount + 1; i < MessageCount; i++)
            peer.Enqueue(AllocationFrameData.Frame([(byte)i]));
        peer.ReleasePump();
        await sink.WaitForAsync(MessageCount);

        // The measured window is single-threaded by construction (see the
        // class doc): WindowDeltas asserts the invariant instead of silently
        // dropping cross-thread samples.
        var deltas = WindowDeltas(sink);

        // The counter on one thread is monotonic while this test owns it.
        deltas.Min().Should().BeGreaterThanOrEqualTo(0);

        AssertPerMessageCostBounded(deltas, medianCeiling: 512);
    }

    [Fact(Timeout = 15_000)]
    public async Task Receive_EachMessage_RentsExactlyOnePooledBuffer()
    {
        using var pool = new CountingRentPool();
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket(new ZSocketOptions { Pool = pool, MessageSink = sink });
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
        await using var socket = new ZPairSocket(new ZSocketOptions { MessageSink = sink });
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));
        var peer = AllocationFakeTransport.Current!;

        peer.Enqueue(AllocationFrameData.Frame([.. "first"u8]));
        await sink.WaitForAsync(1);

        // The first delivery reaches the sink and allocates on the pump thread.
        //
        // Deliberately not asserting a ceiling here. Samples holds
        // GC.GetAllocatedBytesForCurrentThread, which is per-thread and
        // monotonic only within one thread, so the only sound bounds are
        // increments between two samples taken on the same thread - and the
        // steady-state test above establishes those inside a window it pins to
        // a single thread. This test takes a single sample after an await, so
        // the pump may resume on a different pool thread: subtracting across
        // that boundary can even go negative, and an absolute value measures
        // whatever the host already allocated on that thread (under
        // Microsoft.Testing.Platform, discovery and other tests share the same
        // pool) rather than anything about this delivery.
        sink.Samples[0].Should().BeGreaterThan(0);
    }

    [Fact(Timeout = 15_000)]
    public async Task Receive_MultiFrameMessage_PerMessageCostIsBoundedOnPumpThread()
    {
        var sink = new MeasuringSink(MessageCount);
        await using var socket = new ZPairSocket(new ZSocketOptions { MessageSink = sink });
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

        // Same deterministic window as the steady-state test: park the pump
        // with a sentinel frame, buffer the measured two-frame messages, then
        // release for a single-threaded drain.
        peer.ArmSynchronousWindow();
        peer.Enqueue(AllocationFrameData.Frame([0x00]));
        await peer.WaitUntilPumpIdleAsync();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        for (var i = WarmupCount + 1; i < MessageCount; i++)
        {
            peer.Enqueue(AllocationFrameData.Frame(firstFrame, true));
            peer.Enqueue(AllocationFrameData.Frame(secondFrame));
        }

        peer.ReleasePump();
        await sink.WaitForAsync(MessageCount);

        var deltas = WindowDeltas(sink);
        deltas.Min().Should().BeGreaterThanOrEqualTo(0);

        // ~2 pooled rents per two-frame message; the absolute gates are Release
        // only (see AssertPerMessageCostBounded).
        AssertPerMessageCostBounded(deltas, medianCeiling: 1024);
    }

    /// <summary>
    /// Deltas across the measured window with the single-thread invariant
    /// asserted, not filtered: the fake's pump gate parks the pump, the frames
    /// are pre-buffered, and the release lets it drain in one continuation
    /// with synchronously completing reads, so it cannot migrate threads
    /// mid-window. A migration would make the thread-local GC counter
    /// meaningless, so it must fail the test loudly instead of silently
    /// skewing the deltas.
    /// </summary>
    private static long[] WindowDeltas(MeasuringSink sink)
    {
        // The sentinel frame that wakes the pump out of its post-warm-up
        // channel block is delivered on the wake-up continuation, which may be
        // a different thread than the release drain; the measured window
        // therefore starts after the sentinel.
        var windowThreads = sink.ThreadIds[(WarmupCount + 1)..].Distinct().ToArray();
        windowThreads.Should().ContainSingle(
            "the measured window must run on a single pump thread for the thread-local deltas to be valid");

        var deltas = new long[MessageCount - WarmupCount - 2];
        for (var i = WarmupCount + 2; i < MessageCount; i++)
            deltas[i - WarmupCount - 2] = sink.Samples[i] - sink.Samples[i - 1];

        return deltas;
    }

    /// <summary>
    /// Bounds the steady per-message cost, tolerating a small number of one-off
    /// runtime allocations.
    /// <para>
    /// The maximum is deliberately not asserted. Tiered JIT/OSR can allocate
    /// inside an otherwise single-threaded window: measured at 16464 B, once per
    /// process, in the tenth measured window of such a sequence (~20000
    /// cumulative deliveries), and never when tiered compilation is disabled
    /// (0 events across 36 windows). A max gate would therefore fail a healthy
    /// build as soon as the measured delivery count reaches that point; this
    /// class currently performs ~5000, so it clears the event only by margin
    /// rather than by design.
    /// </para>
    /// <para>
    /// A real regression is unaffected by this tolerance: a leak or a larger
    /// per-message allocation moves every delta, so it trips the excursion count
    /// by orders of magnitude, while the median catches an across-the-board
    /// increase.
    /// </para>
    /// </summary>
    private static void AssertPerMessageCostBounded(long[] deltas, long medianCeiling)
    {
#if !DEBUG
        var excursions = deltas.Where(d => d > PerMessageCeiling).ToArray();
        excursions.Length.Should().BeLessThanOrEqualTo(
            RuntimeEventAllowance,
            "one-off JIT/OSR allocations are tolerated, but a per-message regression moves every "
            + "delta past the {0} B ceiling and overshoots this count by orders of magnitude",
            PerMessageCeiling);

        // Asserted as a maximum rather than per-element so a failure names the
        // worst delivery instead of an opaque loop variable.
        var worstExcursion = excursions.Length == 0 ? 0 : excursions.Max();
        worstExcursion.Should().BeLessThanOrEqualTo(
            RuntimeEventCeiling,
            "a single delivery allocating more than {0} B is a regression, not a runtime event",
            RuntimeEventCeiling);

        // The median reflects the steady per-message pool cost, not a heavy tail.
        Median(deltas).Should().BeLessThanOrEqualTo(medianCeiling);
#else
        // Debug boxes async state machines per delivery (measured: ~552 B per
        // send), so the absolute per-message gates only hold in Release
        // (0006 3.6); CI and the allocation gate are Release.
        _ = deltas;
        _ = medianCeiling;
#endif
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
