using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;

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
    private readonly long[] samples = new long[capacity];
    private readonly int[] threadIds = new int[capacity];
    private readonly Lock gate = new();
    private readonly List<(int Threshold, TaskCompletionSource Tcs)> pending = [];
    private int index;
    private long received;

    /// <summary>Per-delivery absolute counters, captured on the pump thread.</summary>
    public long[] Samples => samples;

    /// <summary>Thread id observed at each delivery, for diagnosing pump thread stability.</summary>
    public int[] ThreadIds => threadIds;

    /// <summary>Messages delivered so far.</summary>
    public int Count => (int)Volatile.Read(ref received);

    /// <summary>
    /// Real completion notification: completes when at least <paramref name="count"/>
    /// messages have been delivered, so tests await a task instead of polling.
    /// Completes synchronously when the count is already reached.
    /// </summary>
    public Task WaitForAsync(int count)
    {
        lock (gate)
        {
            if (Volatile.Read(ref received) >= count) return Task.CompletedTask;

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            pending.Add((count, tcs));
            return tcs.Task;
        }
    }

    public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
    {
        samples[index] = GC.GetAllocatedBytesForCurrentThread();
        threadIds[index] = Environment.CurrentManagedThreadId;
        index++;
        Volatile.Write(ref received, index);
        NotifyWaiters();
        message.Dispose();
        return ValueTask.CompletedTask;
    }

    private void NotifyWaiters()
    {
        lock (gate)
        {
            if (pending.Count == 0) return;

            var delivered = Volatile.Read(ref received);
            for (var i = pending.Count - 1; i >= 0; i--)
            {
                if (delivered >= pending[i].Threshold)
                {
                    pending[i].Tcs.TrySetResult();
                    pending.RemoveAt(i);
                }
            }
        }
    }
}
