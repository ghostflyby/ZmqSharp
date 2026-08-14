using ZmqSharp.Patterns;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// SUB composition root (0013): receive-only with a topic-prefix subscription
/// filter. Subscribe to a byte prefix; a message is delivered only when its
/// first frame (the topic) starts with a subscribed prefix. Subscriptions are
/// propagated to connected publishers using libzmq's wire convention: a
/// message whose first frame is <c>0x01</c> + topic subscribes, <c>0x00</c> +
/// topic unsubscribes (so a NetMQ/libzmq publisher starts sending). The
/// subscription set lives in a <see cref="ZTopicFilter"/>; SUB composes a
/// filter inbound policy, XSUB composes pass-through.
/// </summary>
public class ZSubSocket : ZSocketBase
{
    private readonly ZTopicFilter filter;

    public ZSubSocket(ZSocketOptions? options = null)
        : this(options ?? new ZSocketOptions(), ZSocketTypes.Sub, new ZTopicFilter(), true)
    {
    }

    internal ZSubSocket(ZSocketOptions options, ZSocketType type)
        : this(options, type, new ZTopicFilter(), false)
    {
    }

    private ZSubSocket(ZSocketOptions options, ZSocketType type, ZTopicFilter filter, bool filterInbound)
        : base(options, new ZNoDispatch("SUB is receive-only"), type,
            filterInbound ? new ZTopicFilterPolicy(filter) : ZInboundPolicy.PassThrough)
    {
        this.filter = filter;
        SetPeerConnectedHandler(SendSubscriptionsTo);
    }

    /// <summary>Subscribes to a topic prefix; the empty prefix subscribes to everything.</summary>
    public void Subscribe(byte[] topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        filter.Add(topic);
        BroadcastSubscription(0x01, topic);
    }

    /// <summary>Removes a subscription (matched by content).</summary>
    public void Unsubscribe(byte[] topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        filter.RemoveAll(topic);
        BroadcastSubscription(0x00, topic);
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
        foreach (var topic in filter.Snapshot())
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
