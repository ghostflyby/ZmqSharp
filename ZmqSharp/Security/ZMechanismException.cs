namespace ZmqSharp.Security;

/// <summary>
/// A security-mechanism failure: authentication rejection, an unexpected
/// handshake command, or an ERROR command from the peer. Derives from
/// <see cref="ZeroMqProtocolException"/> so the socket pump's existing catch
/// faults establishment unchanged, while callers can still distinguish an
/// authentication failure (0016 section 8).
/// </summary>
public class ZMechanismException : ZeroMqProtocolException
{
    public ZMechanismException(string message)
        : base(message)
    {
    }

    public ZMechanismException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
