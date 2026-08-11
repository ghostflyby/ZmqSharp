using System.Buffers;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// SUB composition root (0013): receive-only with a topic-prefix subscription
/// filter. Subscribe to a byte prefix; a message is delivered only when its
/// first frame (the topic) starts with a subscribed prefix. Subscriptions are
/// propagated to connected publishers using libzmq's wire convention: a
/// message whose first frame is <c>0x01</c> + topic subscribes, <c>0x00</c> +
/// topic unsubscribes (so a NetMQ/libzmq publisher starts sending).
/// </summary>
public class ZSubSocket : ZSocketBase
{
    private readonly List<byte[]> subscriptions = [];

    public ZSubSocket(ZSocketOptions options)
        : this(options, new ZSubCore())
    {
    }

    internal ZSubSocket(ZSocketOptions options, ZSubCore core)
        : base(options, core)
    {
        SetPeerConnectedHandler(SendSubscriptionsTo);
    }

    /// <summary>Subscribes to a topic prefix; the empty prefix subscribes to everything.</summary>
    public void Subscribe(byte[] topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        lock (StateLock)
        {
            subscriptions.Add(topic);
        }

        BroadcastSubscription(0x01, topic);
    }

    /// <summary>Removes a subscription (matched by content).</summary>
    public void Unsubscribe(byte[] topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        lock (StateLock)
        {
            subscriptions.RemoveAll(subscription => subscription.AsSpan().SequenceEqual(topic));
        }

        BroadcastSubscription(0x00, topic);
    }

    /// <summary>Drops messages whose topic frame does not match any subscribed prefix.</summary>
    protected override ZMessage? PrepareInboundForSink(IZConnection peer, ZMessage message)
    {
        var topic = message[0].ToSequence();
        lock (StateLock)
        {
            foreach (var subscription in subscriptions)
                if (subscription.Length <= topic.Length
                    && topic.Slice(0, subscription.Length).ToArray().AsSpan().SequenceEqual(subscription))
                    return message;
        }

        message.Dispose();
        return null;
    }

    /// <summary>libzmq wire convention: first frame 0x01 subscribes, 0x00 unsubscribes.</summary>
    private void BroadcastSubscription(byte marker, byte[] topic)
    {
        foreach (var peer in PeerSnapshot)
        {
            var payload = new byte[1 + topic.Length];
            payload[0] = marker;
            topic.CopyTo(payload, 1);
            var message = new ZMessage(new ZSingleMessage(
                new ZFrame(new ZSegment(payload, 0, payload.Length))));
            _ = SendToAsync(peer, message, CancellationToken.None);
        }
    }

    private void SendSubscriptionsTo(IZConnection peer)
    {
        byte[][] snapshot;
        lock (StateLock)
        {
            snapshot = [.. subscriptions];
        }

        foreach (var topic in snapshot)
        {
            var payload = new byte[1 + topic.Length];
            payload[0] = 0x01;
            topic.CopyTo(payload, 1);
            var message = new ZMessage(new ZSingleMessage(
                new ZFrame(new ZSegment(payload, 0, payload.Length))));
            _ = SendToAsync(peer, message, CancellationToken.None);
        }
    }
}
