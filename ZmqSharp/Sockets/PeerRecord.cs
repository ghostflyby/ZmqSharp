using System.Threading.Channels;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

internal enum PeerPhase { Registered, Handshaking, Established, Stopping, Closed }

/// <summary>The single lifecycle record and resource owner for a peer.</summary>
internal sealed class PeerRecord(IZConnection connection, ZEndpointRegistration registration, bool accepted)
{
    public ZPeer Peer { get; } = new();
    public IZConnection Connection { get; } = connection;
    public ZEndpointRegistration Registration { get; } = registration;
    public bool Accepted { get; } = accepted;
    public Exception? Failure { get; set; }
    public volatile PeerPhase Phase = PeerPhase.Registered;
    public bool HandshakeCounted { get; set; } = accepted;
    public TaskCompletionSource Established { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public ZmtpSession? Session { get; set; }
    public ReceiveMaterializer? Materializer { get; set; }
    public MessageAccumulator Accumulator { get; } = new();
    public Channel<ZMessage>? Queue { get; set; }
    public Lock ReadLock { get; } = new();
    public int ActiveSends { get; set; }
    public TaskCompletionSource? SendsDrained { get; set; }
}

internal sealed class MessageAccumulator
{
    public List<ZFrame> Frames { get; } = [];
}
