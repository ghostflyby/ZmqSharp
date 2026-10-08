using System.Threading.Channels;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;

namespace ZmqSharp;

/// <summary>Queue-capable socket surface; peer queues and cleanup belong to its runtime.</summary>
public abstract class ZQueueSocketBase : ZSocketBase
{
    protected ZQueueSocketBase(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null) : base(new SocketRuntime(options, dispatch, type, inbound, supportsQueue: true)) { }
    private protected ZQueueSocketBase(SocketRuntime runtime) : base(runtime) { }
    public ChannelReader<ZMessage> Messages => Runtime.QueueSurface?.Messages
        ?? throw new InvalidOperationException("no queue composed: remove MessageSink or set ReceiveSurface = ZReceiveSurface.Queue");
    public ChannelWriter<ZMessage>? Outbound => Runtime.QueueSurface?.Outbound;
    public long ReceiveRejections => ReceiveRejectionsCount;
}
