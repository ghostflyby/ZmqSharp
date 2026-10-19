using System.Threading.Channels;

namespace ZmqSharp.Sockets;

/// <summary>Queue projection of runtime-owned records; it owns no peer registry.</summary>
internal sealed class ReceiveQueueSurface
{
    private readonly SocketRuntime _runtime;
    private readonly ZQueueFactory _receiveFactory;
    private readonly Channel<ZMessage>? _outbound;
    private readonly WakeGate _wake = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile PeerRecord[] _snapshot = [];
    public ChannelReader<ZMessage> Messages { get; }
    public ChannelWriter<ZMessage>? Outbound => _outbound?.Writer;

    public ReceiveQueueSurface(SocketRuntime runtime, ZSocketOptions options)
    {
        _runtime = runtime;
        _receiveFactory = options.ReceiveQueueFactory;
        Messages = new AggregateReader(() => _snapshot, _wake, _completion.Task);
        if (options.SendQueueFactory is not { } factory) return;

        _outbound = factory.Create(static message => message.Dispose());
        runtime.TrackBackground(SendPumpAsync(runtime.LifetimeToken));
    }

    public void Add(PeerRecord record)
    {
        record.Queue = _receiveFactory.Create(static message => message.Dispose());
        _snapshot = [.. _snapshot, record];
    }

    public void Remove(PeerRecord record)
    {
        var current = _snapshot;
        var index = Array.IndexOf(current, record);
        if (index < 0) return;
        var updated = new PeerRecord[current.Length - 1];
        current.AsSpan(0, index).CopyTo(updated);
        current.AsSpan(index + 1).CopyTo(updated.AsSpan(index));
        _snapshot = updated;
        _wake.Wake();
    }

    public async ValueTask DeliverAsync(PeerRecord record, ZMessage message, CancellationToken token)
    {
        if (record.Queue is not { } queue || record.Phase >= PeerPhase.Stopping)
        {
            message.Dispose();
            return;
        }

        try
        {
            await queue.Writer.WriteAsync(message, token);
        }
        catch
        {
            message.Dispose();
            throw;
        }

        if (queue.Reader.Count == 1) _wake.Wake();
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

        if (failure is not null && _snapshot.Length == 0) _outbound?.Writer.TryComplete(failure);
    }

    public void Stop() => _outbound?.Writer.TryComplete();

    public void Complete()
    {
        if (_outbound is not null)
            while (_outbound.Reader.TryRead(out var message))
                message.Dispose();
        _completion.TrySetResult();
    }

    private async Task SendPumpAsync(CancellationToken token)
    {
        if (_outbound is null) return;
        try
        {
            await foreach (var message in _outbound.Reader.ReadAllAsync(token))
                await _runtime.SendAsyncCore(message, token);
        }
        catch (Exception failure) when (failure is OperationCanceledException or ChannelClosedException) { }
        catch (Exception failure)
        {
            _outbound.Writer.TryComplete(failure);
        }
    }
}
