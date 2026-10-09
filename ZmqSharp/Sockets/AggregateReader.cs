using System.Threading.Channels;

namespace ZmqSharp.Sockets;

internal sealed class AggregateReader(Func<PeerRecord[]> snapshot, WakeGate wake, Task completion) : ChannelReader<ZMessage>
{
    public override Task Completion => completion;

    public override bool TryRead(out ZMessage item)
    {
        foreach (var record in snapshot())
            lock (record.ReadLock)
            {
                if (record.Phase != PeerPhase.Established || record.Queue is not { } queue) continue;
                if (queue.Reader.TryRead(out item)) return true;
            }

        item = default;
        return false;
    }

    public override async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        if (completion.IsCompleted) return false;
        var captured = wake.Capture();
        foreach (var record in snapshot())
            if (record is { Phase: PeerPhase.Established, Queue: { Reader.Count: > 0 } })
                return true;
        var done = await Task.WhenAny(captured, completion).WaitAsync(cancellationToken);
        return done == captured && !completion.IsCompleted;
    }
}
