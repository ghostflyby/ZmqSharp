using System.Threading.Channels;

namespace ZmqSharp.Sockets;

/// <summary>Queue projection of runtime-owned records; it owns no peer registry.</summary>
internal sealed class ReceiveQueueSurface
{
    private readonly SocketRuntime runtime;
    private readonly ZQueueFactory receiveFactory;
    private readonly Channel<ZMessage>? outbound;
    private readonly WakeGate wake = new();
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile PeerRecord[] snapshot = [];
    public ChannelReader<ZMessage> Messages { get; }
    public ChannelWriter<ZMessage>? Outbound => outbound?.Writer;

    public ReceiveQueueSurface(SocketRuntime runtime, ZSocketOptions options)
    {
        this.runtime = runtime;
        receiveFactory = options.ReceiveQueueFactory;
        Messages = new AggregateReader(() => snapshot, wake, completion.Task);
        if (options.SendQueueFactory is { } factory)
        {
            outbound = factory.Create(static message => message.Dispose());
            runtime.TrackBackground(SendPumpAsync(runtime.LifetimeToken));
        }
    }

    public void Add(PeerRecord record)
    {
        record.Queue = receiveFactory.Create(static message => message.Dispose());
        snapshot = [.. snapshot, record];
    }
    public void Remove(PeerRecord record)
    {
        var current = snapshot;
        var index = Array.IndexOf(current, record);
        if (index < 0) return;
        var updated = new PeerRecord[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        snapshot = updated;
        wake.Wake();
    }
    public async ValueTask DeliverAsync(PeerRecord record, ZMessage message, CancellationToken token)
    {
        if (record.Queue is not { } queue || record.Phase >= PeerPhase.Stopping)
        {
            message.Dispose();
            return;
        }
        try { await queue.Writer.WriteAsync(message, token); }
        catch { message.Dispose(); throw; }
        if (queue.Reader.Count == 1) wake.Wake();
    }
    public void Reclaim(PeerRecord record, Exception? failure)
    {
        lock (record.ReadLock)
        {
            if (record.Queue is { } queue)
            {
                queue.Writer.TryComplete();
                while (queue.Reader.TryRead(out var message)) message.Dispose();
                record.Queue = null;
            }
        }
        if (failure is not null && snapshot.Length == 0) outbound?.Writer.TryComplete(failure);
    }
    public void Stop() => outbound?.Writer.TryComplete();
    public void Complete()
    {
        if (outbound is { } queue)
            while (queue.Reader.TryRead(out var message)) message.Dispose();
        completion.TrySetResult();
    }
    private async Task SendPumpAsync(CancellationToken token)
    {
        if (outbound is not { } channel) return;
        try
        {
            await foreach (var message in channel.Reader.ReadAllAsync(token))
                await runtime.SendAsyncCore(message, token);
        }
        catch (Exception failure) when (failure is OperationCanceledException or ChannelClosedException) { }
        catch (Exception failure) { channel.Writer.TryComplete(failure); }
    }
}
