using System.Buffers;
using ZmqSharp.Patterns;

namespace ZmqSharp.Sockets;

/// <summary>
/// SUB/XSUB's subscription set with prefix matching (0013). Shared by SUB
/// (topic filter) and XSUB (subscription propagation without filtering);
/// the set's own lock keeps the filter policy and the Subscribe/Unsubscribe
/// API thread-safe without the socket state lock.
/// </summary>
internal sealed class ZTopicFilter
{
    private readonly Lock _filterLock = new();
    private readonly List<byte[]> _subscriptions = [];

    public byte[] Add(ReadOnlySpan<byte> topic)
    {
        var copy = topic.ToArray();
        lock (_filterLock) _subscriptions.Add(copy);
        return copy;
    }

    public void RemoveAll(ReadOnlySpan<byte> topic)
    {
        lock (_filterLock)
        {
            for (var i = _subscriptions.Count - 1; i >= 0; i--)
                if (_subscriptions[i].AsSpan().SequenceEqual(topic))
                    _subscriptions.RemoveAt(i);
        }
    }

    /// <summary>True when the topic starts with any subscribed prefix.</summary>
    public bool Matches(ReadOnlySequence<byte> topic)
    {
        lock (_filterLock)
        {
            foreach (var subscription in _subscriptions)
                if (subscription.Length <= topic.Length
                    && topic.Slice(0, subscription.Length).ToArray().AsSpan().SequenceEqual(subscription))
                    return true;
        }

        return false;
    }

    /// <summary>Copies the subscribed prefixes (for outbound subscription propagation).</summary>
    public byte[][] Snapshot()
    {
        lock (_filterLock) return [.. _subscriptions];
    }
}

/// <summary>
/// SUB's inbound policy: delivers messages whose topic frame matches a
/// subscribed prefix and drops the rest (disposing them).
/// </summary>
internal sealed class ZTopicFilterPolicy(ZTopicFilter filter) : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(ZPeer peer, ZMessage message, CancellationToken token)
    {
        if (filter.Matches(message[0].ToSequence())) return ValueTask.FromResult(ZInboundDecision.Deliver());

        message.Dispose();
        return ValueTask.FromResult(ZInboundDecision.Drop());
    }
}
