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
public abstract class ZAsyncState
{
    protected readonly MemoryPool<byte> Pool;
    protected readonly Lock StateLock = new();
    protected readonly List<Task> BackgroundTasks = [];
    protected readonly CancellationTokenSource Cts = new();
    protected int Closed;

    protected ZAsyncState(MemoryPool<byte> pool)
    {
        Pool = pool;
    }

    protected void TrackBackground(Task task)
    {
        lock (StateLock)
        {
            BackgroundTasks.Add(task);
        }
    }

    protected async Task AwaitBackgroundAsync(CancellationToken token)
    {
        Task[] tasks;
        lock (StateLock)
        {
            tasks = [.. BackgroundTasks];
        }

        if (tasks.Length == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(tasks).WaitAsync(token);
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
        if (Volatile.Read(ref Closed) == 1)
        {
            throw new ObjectDisposedException(GetType().Name);
        }
    }
}
