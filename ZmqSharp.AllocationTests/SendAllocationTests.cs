using System.Net;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Allocation measurements of the send hot path. The fake transport's writes
/// complete synchronously, so each SendAsync runs its whole chain on the
/// calling thread and GC.GetAllocatedBytesForCurrentThread deltas are valid.
/// <para>
/// That synchronous-completion property is what makes the reading valid, so the
/// measurement window asserts the thread it depends on instead of assuming it:
/// a window that migrated across thread-pool threads would compare two
/// different threads' counters, and a cross-thread subtraction is not merely
/// noisy but can be negative - which would satisfy an allocation ceiling and
/// report "allocation-free" for a path that regressed to asynchronous. The
/// dedicated project plus a non-parallel collection keep the window clean.
/// </para>
/// </summary>
[Collection("allocation-measurement")]
public class SendAllocationTests
{
    [Fact]
    public async Task Send_SteadyState_IsAllocationFreePerMessage()
    {
        await using var socket = new ZPairSocket();
        await socket.ConnectAsync<EndPoint, AllocationFakeTransport>(
            new IPEndPoint(IPAddress.Loopback, 0));

        const int count = 512;
        var messages = new ZMessage[count];
        for (var i = 0; i < count; i++) messages[i] = ZMessage.FromOwned([(byte)i]);

        // Warm up: the first sends pay one-time costs (delegate caches,
        // tiered JIT), which would pollute the measurement.
        for (var i = 0; i < 16; i++) await socket.SendAsync(messages[i]);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // The measured window: record the thread on both sides of every send,
        // so a migration inside the window fails the test loudly rather than
        // silently producing a meaningless (possibly negative) delta.
        var windowThread = Environment.CurrentManagedThreadId;
        var windowThreadStable = true;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 16; i < count; i++)
        {
            await socket.SendAsync(messages[i]);
            if (Environment.CurrentManagedThreadId != windowThread) windowThreadStable = false;
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        windowThreadStable.Should().BeTrue(
            "the thread-local GC delta is only valid while the whole measured window runs on one thread");

        // A real per-message allocation would be ~48 bytes minimum (a boxed
        // state machine); a whole-message budget of a few hundred bytes with
        // amortized warm-up noise proves the steady state is allocation-free.
#if !DEBUG
        allocated.Should().BeGreaterThanOrEqualTo(0,
            "a negative delta means the window spanned two threads, not that the path allocated nothing");
        allocated.Should().BeLessThan(1024);
#else
        // Debug boxes async state machines per call (~48 B), so the absolute
        // gate only holds in Release (0006 3.6); the CI run is Release.
        _ = allocated;
#endif

        foreach (var message in messages) message.Dispose();
    }
}
