using System.Net;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Allocation measurements of the send hot path. The fake transport's writes
/// complete synchronously, so each SendAsync runs its whole chain on the
/// calling thread and GC.GetAllocatedBytesForCurrentThread deltas are valid
/// (the same single-thread window the in-suite test relies on; the dedicated
/// project plus a non-parallel collection keep the window clean).
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
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 16; i < count; i++) await socket.SendAsync(messages[i]);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // A real per-message allocation would be ~48 bytes minimum (a boxed
        // state machine); a whole-message budget of a few hundred bytes with
        // amortized warm-up noise proves the steady state is allocation-free.
#if !DEBUG
        allocated.Should().BeLessThan(1024);
#else
        // Debug boxes async state machines per call (~48 B), so the absolute
        // gate only holds in Release (0006 3.6); the CI run is Release.
        _ = allocated;
#endif

        foreach (var message in messages) message.Dispose();
    }
}
