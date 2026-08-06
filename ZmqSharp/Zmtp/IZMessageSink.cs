using ZmqSharp.Messages;

namespace ZmqSharp.Zmtp;

public delegate ZReceiveAction? ZMessageDecider(in ZReceiveContext context);

/// <summary>
/// Borrowed callback: return true to keep receiving; return false to pause the
/// receive pump, resumed via ZmtpParser.Resume(). The view is valid only during
/// the call.
/// </summary>
public delegate bool ZBorrowedMessageHandler(ZMessageView message, CancellationToken token);

/// <summary>
/// Owned callback: return true to accept (ownership transfers to the caller,
/// which must Dispose); return false to reject (the parser disposes and continues).
/// </summary>
public delegate bool ZOwnedMessageHandler(ZMessage message, CancellationToken token);

/// <summary>Receiver of parsed messages.</summary>
public interface IZMessageSink
{
    /// <summary>Return null to fall back to the ZReceiveOptions default policy.</summary>
    ZReceiveAction? Decide(in ZReceiveContext context);

    bool OnBorrowed(ZMessageView message, CancellationToken token);

    bool OnOwned(ZMessage message, CancellationToken token);
}

/// <summary>
/// Convenient delegate-based receiver; missing callbacks default to accepting
/// (borrowed continues, owned accepts).
/// </summary>
public sealed class ZCallbackSink(
    ZBorrowedMessageHandler? borrowed = null,
    ZOwnedMessageHandler? owned = null,
    ZMessageDecider? decide = null)
    : IZMessageSink
{
    public ZReceiveAction? Decide(in ZReceiveContext context) => decide?.Invoke(context);

    public bool OnBorrowed(ZMessageView message, CancellationToken token)
        => borrowed?.Invoke(message, token) ?? true;

    public bool OnOwned(ZMessage message, CancellationToken token)
        => owned?.Invoke(message, token) ?? true;
}
