using FluentAssertions;
using Xunit;
using ZmqSharp.Security.Curve;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Allocation measurements of the CURVE traffic path (0023): sealing and
/// opening reuse per-connection buffers and the backend is allocation-free,
/// so steady-state frames allocate nothing. The fake connection's reads and
/// writes complete synchronously, so each operation runs its whole chain on
/// the calling thread and GC.GetAllocatedBytesForCurrentThread deltas are
/// valid.
/// <para>
/// That synchronous-completion property is what makes each reading valid, so
/// both windows assert the thread they depend on rather than assuming it: a
/// window that migrated across thread-pool threads compares two threads'
/// counters, and a cross-thread subtraction can be negative - which would
/// satisfy an allocation ceiling and report "allocation-free" for a path that
/// regressed to asynchronous.
/// </para>
/// </summary>
[Collection("allocation-measurement")]
public class CurveTrafficAllocationTests
{
    [Fact]
    public async Task CurveTraffic_SealAndOpen_AreAllocationFreePerFrame()
    {
        var crypto = new BouncyCastleCurveCrypto();
        var sessionKey = Key32.From(new byte[32]); // any key; the loop is symmetric
        var prefix = new byte[16]; // matching nonce prefix on both ends

        const int count = 1000;
        const int warmup = 16;

        var sealerRaw = new CurveFakeConnection(writeCapacity: (count + warmup) * 64);
        var sealer = new CurveSessionConnection(sealerRaw, crypto, sessionKey, prefix, prefix, 1, 0);
        byte[] payload = [.. "hello-curve"u8];

        // Warm up: the first seals pay one-time costs (JIT, stackalloc paths).
        for (var i = 0; i < warmup; i++) await sealer.SendFrameAsync(payload, false);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var sealThread = Environment.CurrentManagedThreadId;
        var sealThreadStable = true;
        var beforeSeal = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < count; i++)
        {
            await sealer.SendFrameAsync(payload, false);
            if (Environment.CurrentManagedThreadId != sealThread) sealThreadStable = false;
        }
        var sealAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeSeal;

        // Replay the recorded wire frames through an opener. The sealer's
        // nonce tails are 1..count+warmup (increasing), the opener's decode
        // nonce starts at 0, so every frame is accepted. The recording
        // snapshot is taken outside the measured window.
        var wire = sealerRaw.ToArray();
        var openerRaw = new CurveFakeConnection(wire);
        var opener = new CurveSessionConnection(openerRaw, crypto, sessionKey, prefix, prefix, count + warmup + 1, 0);

        var buffer = new byte[4096];
        for (var i = 0; i < warmup; i++) await opener.ReadAsync(buffer);
        for (var i = 0; i < warmup; i++) await sealer.SendFrameAsync(payload, false);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var openThread = Environment.CurrentManagedThreadId;
        var openThreadStable = true;
        var beforeOpen = GC.GetAllocatedBytesForCurrentThread();
        var totalRead = 0;
        for (var i = 0; i < count; i++)
        {
            totalRead += await opener.ReadAsync(buffer);
            if (Environment.CurrentManagedThreadId != openThread) openThreadStable = false;
        }
        var openAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeOpen;
        totalRead.Should().BeGreaterThan(0);

        sealThreadStable.Should().BeTrue(
            "the thread-local seal delta is only valid while the whole measured window runs on one thread");
        openThreadStable.Should().BeTrue(
            "the thread-local open delta is only valid while the whole measured window runs on one thread");

        // A per-frame allocation would be a few hundred bytes minimum (a
        // rented buffer or an array); both loops completing under the gate
        // proves the CURVE traffic path is steady-state allocation-free (0023).
#if !DEBUG
        sealAllocated.Should().BeGreaterThanOrEqualTo(0,
            "a negative seal delta means the window spanned two threads, not that the path allocated nothing");
        openAllocated.Should().BeGreaterThanOrEqualTo(0,
            "a negative open delta means the window spanned two threads, not that the path allocated nothing");
        sealAllocated.Should().BeLessThan(1024);
        openAllocated.Should().BeLessThan(1024);
#else
        _ = sealAllocated;
        _ = openAllocated;
#endif
    }

    /// <summary>
    /// Connection whose reads serve a fixed byte stream and whose writes are
    /// recorded into a pre-sized list, both completing synchronously so the
    /// measurement window stays on the calling thread and the recording
    /// itself allocates nothing after warm-up.
    /// </summary>
    private sealed class CurveFakeConnection : IZConnection
    {
        private readonly List<byte> writes;
        private readonly byte[] feed;
        private int position;

        public CurveFakeConnection(byte[]? feed = null, int writeCapacity = 0)
        {
            this.feed = feed ?? [];
            writes = new List<byte>(writeCapacity);
        }

        /// <summary>A snapshot of all recorded wire bytes (allocates; call outside the measured window).</summary>
        public byte[] ToArray() => writes.ToArray();

        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        {
            var count = Math.Min(buffer.Length, feed.Length - position);
            feed.AsSpan(position, count).CopyTo(buffer.Span);
            position += count;
            return ValueTask.FromResult(count);
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            writes.AddRange(bytes.Span);
            return ValueTask.CompletedTask;
        }

        public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
        {
            return WriteAsync(frame, token);
        }

        public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        {
            return WriteAsync(body, token);
        }

        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            return ValueTask.CompletedTask;
        }

        public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
        {
        }

        public void SetConnectionEndedHandler(Action onConnectionEnded)
        {
        }

        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
        {
            return ValueTask.FromResult(true);
        }

        public void OnConnectionEnded()
        {
        }

        public void Dispose()
        {
        }
    }
}
