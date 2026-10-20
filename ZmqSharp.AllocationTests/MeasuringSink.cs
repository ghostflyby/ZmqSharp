using Xunit;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Message sink that samples the receiving pump thread's own allocation
/// counter at each delivery. OnMessageAsync runs synchronously on the pump
/// thread (0007 2.3), so the delta between two consecutive samples is exactly
/// what parsing + materializing + delivering that message allocated on that
/// thread - the read-side window that a caller-thread counter cannot see.
/// The sample is taken before disposal, which only returns pooled segments to
/// the pool and allocates nothing.
/// </summary>
internal sealed class MeasuringSink(int capacity) : IPatternSink
{
    private readonly Lock _gate = new();
    private readonly List<(int Threshold, TaskCompletionSource Tcs)> _pending = [];

    // Delivered count and write cursor; both guarded by gate. OnMessageAsync
    // is the single writer (the pump thread) but still publishes under the
    // lock, so every access is uniformly synchronized and the test thread
    // reading Samples/ThreadIds after awaiting WaitForAsync sees them via the
    // monitor's memory barrier.
    private int _index;
    private long _received;

    /// <summary>Per-delivery absolute counters, captured on the pump thread.</summary>
    public long[] Samples { get; } = new long[capacity];

    /// <summary>Thread id observed at each delivery, for diagnosing pump thread stability.</summary>
    public int[] ThreadIds { get; } = new int[capacity];

    /// <summary>
    /// Real completion notification: completes when at least <paramref name="count"/>
    /// messages have been delivered, so tests await a task instead of polling.
    /// Completes synchronously when the count is already reached.
    /// </summary>
    public Task WaitForAsync(int count)
    {
        lock (_gate)
        {
            if (_received >= count) return Task.CompletedTask;

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending.Add((count, tcs));
            return tcs.Task.WaitAsync(TestContext.Current.CancellationToken);
        }
    }

    public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
    {
        // Sample before touching the lock: the counters must measure only the
        // library's per-message work, and the cursor is written by this pump
        // thread alone.
        Samples[_index] = GC.GetAllocatedBytesForCurrentThread();
        ThreadIds[_index] = Environment.CurrentManagedThreadId;

        lock (_gate)
        {
            _index++;
            _received = _index;
            NotifyWaitersLocked();
        }

        message.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Caller must hold <c>gate</c>.</summary>
    private void NotifyWaitersLocked()
    {
        if (_pending.Count == 0) return;

        for (var i = _pending.Count - 1; i >= 0; i--)
            if (_received >= _pending[i].Threshold)
            {
                _pending[i].Tcs.TrySetResult();
                _pending.RemoveAt(i);
            }
    }
}
