namespace ZmqSharp.Transports;

/// <summary>
/// A byte connection allowing one reader and one writer concurrently (0034).
/// Every byte connection is also a message connection: the message surface
/// converts whole messages to the byte stream in the connection's framing
/// dialect, so a byte transport is usable wherever the runtime consumes
/// messages. <see cref="ZByteConnection"/> supplies that shared
/// implementation.
/// </summary>
public interface IZConnection : IZMessageConnection, IZByteReader, IZByteWriter
{
}
