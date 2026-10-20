namespace ZmqSharp;

/// <summary>Thrown on ZMTP protocol violations (bad signature, invalid frame, ERROR command, etc.).</summary>
public class ZeroMqProtocolException : InvalidOperationException
{
    public ZeroMqProtocolException(string message)
        : base(message)
    {
    }

    public ZeroMqProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
