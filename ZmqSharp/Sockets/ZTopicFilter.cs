using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// SUB/XSUB's subscription set with prefix matching (0013). Shared by SUB
/// (topic filter) and XSUB (subscription propagation without filtering);
/// the set's own lock keeps the filter policy and the Subscribe/Unsubscribe
/// API thread-safe without the socket state lock.
/// </summary>
internal sealed class ZTopicFilter
{
    private readonly Lock filterLock = new();
    private readonly List<byte[]> subscriptions = [];

    public void Add(byte[] topic)
    {
        lock (filterLock) subscriptions.Add(topic);
    }

    public void RemoveAll(ReadOnlySpan<byte> topic)
    {
        lock (filterLock)
        {
            for (var i = subscriptions.Count - 1; i >= 0; i--)
                if (subscriptions[i].AsSpan().SequenceEqual(topic))
                    subscriptions.RemoveAt(i);
        }
    }

    /// <summary>True when the topic starts with any subscribed prefix.</summary>
    public bool Matches(ReadOnlySequence<byte> topic)
    {
        lock (filterLock)
        {
            foreach (var subscription in subscriptions)
                if (subscription.Length <= topic.Length
                    && topic.Slice(0, subscription.Length).ToArray().AsSpan().SequenceEqual(subscription))
                    return true;
        }

        return false;
    }

    /// <summary>Copies the subscribed prefixes (for outbound subscription propagation).</summary>
    public byte[][] Snapshot()
    {
        lock (filterLock) return [.. subscriptions];
    }
}

/// <summary>
/// SUB's inbound policy: delivers messages whose topic frame matches a
/// subscribed prefix and drops the rest (disposing them).
/// </summary>
internal sealed class ZTopicFilterPolicy(ZTopicFilter filter) : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        if (!filter.Matches(message[0].ToSequence()))
        {
            message.Dispose();
            return ValueTask.FromResult(ZInboundDecision.Drop());
        }

        return ValueTask.FromResult(ZInboundDecision.Deliver());
    }
}
