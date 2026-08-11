using System.Buffers;
using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>Concrete full-duplex connection over a transport stream.</summary>
internal sealed class ZConnection(Stream stream) : IZConnection
{
    private readonly ZmtpFrameEncoder encoder = new(stream);
    private readonly SemaphoreSlim writeGate = new(1, 1);
    private Func<ZFrame, CancellationToken, ValueTask<bool>>? onFrame;
    private Action? onConnectionEnded;
    private int disposed;

    public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> handler)
    {
        onFrame = handler;
    }

    public void SetConnectionEndedHandler(Action handler)
    {
        onConnectionEnded = handler;
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        return stream.ReadAsync(buffer, token);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        await writeGate.WaitAsync(token);
        try
        {
            await stream.WriteAsync(bytes, token);
        }
        finally
        {
            writeGate.Release();
        }
    }

    public async ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
    {
        await writeGate.WaitAsync(token);
        try
        {
            await encoder.WriteFrameAsync(new ReadOnlySequence<byte>(frame), more, token);
        }
        finally
        {
            writeGate.Release();
        }
    }

    public async ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        await writeGate.WaitAsync(token);
        try
        {
            await encoder.WriteCommandAsync(body, token);
        }
        finally
        {
            writeGate.Release();
        }
    }

    public async ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        await writeGate.WaitAsync(token);
        try
        {
            await encoder.WriteMessageAsync(message, token);
        }
        finally
        {
            writeGate.Release();
        }
    }

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        return onFrame?.Invoke(frame, token) ?? ValueTask.FromResult(true);
    }

    public void OnConnectionEnded()
    {
        onConnectionEnded?.Invoke();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        writeGate.Dispose();
        stream.Dispose();
    }
}
