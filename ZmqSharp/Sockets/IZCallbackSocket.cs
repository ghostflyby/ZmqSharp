using ZmqSharp.Zmtp;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>Callback receive surface: borrowed streaming frames.</summary>
public interface IZCallbackSocket : IZSocket
{
    /// <summary>
    /// Borrowed streaming callback: invoked once per frame as it arrives.
    /// Return false to pause this connection's receive pump (backpressure);
    /// resume via <see cref="ResumePaused"/>.
    /// </summary>
    event ZFrameHandler? OnFrame;

    /// <summary>Raised when a peer connection ends; null = clean EOF, otherwise the failure.</summary>
    event Action<IZConnection, Exception?>? PeerEnded;

    /// <summary>Resumes every peer receive pump paused by a false <see cref="OnFrame"/> return.</summary>
    void ResumePaused();
}
