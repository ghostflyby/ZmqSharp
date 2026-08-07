using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Low-level callback surface: receive via the borrowed frame callback.
/// Queue semantics live in ZQueueSocket.
/// </summary>
public interface IZSocket : IZSocketBase
{
    /// <summary>
    /// Borrowed streaming callback: invoked once per frame as it arrives.
    /// Return false to pause this connection's receive pump (backpressure);
    /// resume via <see cref="ResumePaused"/>.
    /// </summary>
    event ZFrameHandler? OnFrame;

    /// <summary>Raised when a peer connection ends; null = clean EOF, otherwise the failure.</summary>
    event Action<Exception?>? PeerEnded;

    /// <summary>Resumes every peer receive pump paused by a false <see cref="OnFrame"/> return.</summary>
    void ResumePaused();

}
