using System.Buffers;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Shared asynchronous lifecycle infrastructure: memory pool, background task
/// tracking, cancellation, and the closed flag. Orthogonal to receive
/// semantics; used by both the callback and queue socket implementations.
/// Public only because public socket types derive from it; the members are
/// protected and are not part of the consumer-facing API.
/// </summary>
public abstract class ZAsyncState(MemoryPool<byte> pool)
{
    protected readonly MemoryPool<byte> Pool = pool;
    protected readonly Lock StateLock = new();
    protected readonly List<Task> BackgroundTasks = [];
    protected readonly CancellationTokenSource Cts = new();
    protected int Closed;

    protected void TrackBackground(Task task)
    {
        lock (StateLock)
        {
            BackgroundTasks.Add(task);
        }
    }

    protected async Task AwaitBackgroundAsync()
    {
        Task[] tasks;
        lock (StateLock)
        {
            tasks = [.. BackgroundTasks];
        }

        if (tasks.Length == 0) return;

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ZeroMqProtocolException)
        {
        }
    }

    protected void ThrowIfClosed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref Closed) == 1, this);
    }
}
