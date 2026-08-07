using System.Buffers;
using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>Concrete full-duplex connection over a transport stream.</summary>
internal sealed class ZConnection(Stream stream) : IZConnection
{
    private readonly ZmtpFrameEncoder encoder = new(stream);
    private Func<ZFrame, CancellationToken, bool>? onFrame;
    private Action? onConnectionEnded;
    private int disposed;

    public void SetFrameHandler(Func<ZFrame, CancellationToken, bool> handler) => onFrame = handler;

    public void SetConnectionEndedHandler(Action handler) => onConnectionEnded = handler;

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        => stream.ReadAsync(buffer, token);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => stream.WriteAsync(bytes, token);

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
        => encoder.WriteFrameAsync(new ReadOnlySequence<byte>(frame), more, token);

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        => encoder.WriteCommandAsync(body, token);

    public ValueTask SendAsync(IZMessage message, CancellationToken token = default)
        => encoder.WriteMessageAsync(message, token);

    public bool OnFrame(ZFrame frame, CancellationToken token)
        => onFrame?.Invoke(frame, token) ?? true;

    public void OnConnectionEnded() => onConnectionEnded?.Invoke();

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            stream.Dispose();
        }
    }
}
