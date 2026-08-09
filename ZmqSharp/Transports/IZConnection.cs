using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// Full-duplex connection: raw write for handshakes, frame/message send methods,
/// and the parser's receive callbacks. No handshake is built in; the driver
/// composes Send and Receive so security mechanisms can vary freely.
/// </summary>
public interface IZConnection : IZMessageSink, IDisposable
{
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default);

    ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);

    ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default);

    ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    ValueTask SendAsync(ZMessage message, CancellationToken token = default);

    void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame);

    void SetConnectionEndedHandler(Action onConnectionEnded);
}
