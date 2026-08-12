
namespace ZmqSharp.Zmtp;

/// <summary>
/// Low-level synchronous streaming callback: invoked once per frame as it is
/// read. The frame is borrowed and valid only during the call. Return true to
/// keep receiving; return false to pause the receive pump (resumed via
/// ZmtpParser.Resume()). This is the borrowed-tier surface (0002); the
/// delivery chain's async seam is ZFrameHandlerAsync.
/// </summary>
public delegate bool ZFrameHandler(ZFrame frame, CancellationToken token);

/// <summary>
/// Asynchronous delivery seam: invoked once per frame as it is read. The frame
/// is borrowed and valid until the returned ValueTask completes. false pauses
/// the receive pump (resumed via ZmtpParser.Resume()); a pending ValueTask
/// pauses it until the task completes. true (or a completed task) continues.
/// </summary>
public delegate ValueTask<bool> ZFrameHandlerAsync(ZFrame frame, CancellationToken token);

/// <summary>Receiver of parsed frames.</summary>
public interface IZMessageSink
{
    ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token);

    /// <summary>Called when the connection ends; discard any partial message and release buffers.</summary>
    void OnConnectionEnded();
}
