using ZmqSharp.Messages;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Low-level streaming callback: invoked once per frame as it is read. The frame
/// is borrowed and valid only during the call. Return true to keep receiving;
/// return false to pause the receive pump (resumed via ZmtpParser.Resume()).
/// </summary>
public delegate bool ZFrameHandler(ZFrame frame, CancellationToken token);

/// <summary>Receiver of parsed frames.</summary>
public interface IZMessageSink
{
    bool OnFrame(ZFrame frame, CancellationToken token);

    /// <summary>Called when the connection ends; discard any partial message and release buffers.</summary>
    void OnConnectionEnded();
}
