using System.Threading.Channels;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

internal enum PeerPhase
{
    Registered,
    Handshaking,
    Established,
    Stopping,
    Closed,
}

/// <summary>
/// The single lifecycle record and resource owner for a peer. The peer is
/// hosted at the frame layer (0033): <see cref="Frames"/> is the frame
/// channel (native, or a framing adapter over <see cref="Bytes"/>); the
/// protocol session and pump run over it. Aborting and disposing the frame
/// connection releases the byte origin with it.
/// </summary>
internal sealed class PeerRecord(IZFrameConnection frames, IZConnection? bytes,
    ZEndpointRegistration registration, bool accepted)
{
    public ZPeer Peer { get; } = new();

    /// <summary>
    /// The frame channel: the establishment adapter initially; the runtime
    /// re-adapts the byte origin after establishment (codec-aware bounds and
    /// the zero-copy allocator, 0033).
    /// </summary>
    public IZFrameConnection Frames { get; internal set; } = frames;
    public IZConnection? Bytes { get; } = bytes;
    public ZEndpointRegistration Registration { get; } = registration;
    public bool Accepted { get; } = accepted;
    public Exception? Failure { get; set; }
    public volatile PeerPhase Phase = PeerPhase.Registered;
    public bool HandshakeCounted { get; set; } = accepted;
    public TaskCompletionSource Established { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public IZWireSession? Session { get; set; }
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
