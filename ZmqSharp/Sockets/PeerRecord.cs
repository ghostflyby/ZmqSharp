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
internal sealed class PeerRecord(IZMessageConnection frames,
    ZEndpointRegistration registration, bool accepted)
{
    public ZPeer Peer { get; } = new();

    /// <summary>
    /// The connection: a byte transport's message-capable connection or a
    /// native message connection. The runtime applies the codec-aware bound
    /// and the zero-copy allocator in place after establishment (0034).
    /// </summary>
    public IZMessageConnection Frames { get; internal set; } = frames;

    /// <summary>The raw byte origin when the connection is byte-backed.</summary>
    public IZConnection? Bytes => Frames.ByteOrigin;
    public ZEndpointRegistration Registration { get; } = registration;
    public bool Accepted { get; } = accepted;
    public Exception? Failure { get; set; }
    public volatile PeerPhase Phase = PeerPhase.Registered;
    public bool HandshakeCounted { get; set; } = accepted;
    public TaskCompletionSource Established { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public IZWireSession? Session { get; set; }
    public ReceiveMaterializer? Materializer { get; set; }
    public Channel<ZMessage>? Queue { get; set; }
    public Lock ReadLock { get; } = new();
    public int ActiveSends { get; set; }
    public TaskCompletionSource? SendsDrained { get; set; }
}
