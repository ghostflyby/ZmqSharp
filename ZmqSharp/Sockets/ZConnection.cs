using System.Buffers;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>Internal per-peer session: byte channel + parser + encoder + queue.</summary>
internal sealed class ZConnection(IZTransport transport, MemoryPool<byte> pool)
    : IAsyncDisposable
{
    private readonly ZmtpParser parser = new(
        transport.Stream ?? throw new InvalidOperationException("connection requires a connected transport"),
        pool);
    private readonly ZmtpFrameEncoder encoder = new(
        transport.Stream ?? throw new InvalidOperationException("connection requires a connected transport"));
    private readonly SemaphoreSlim sendGate = new(1, 1);
    private int closed;

    public IZMessageSink? Sink { get; set; }

    public Task? RunTask { get; set; }

    public string Endpoint { get; set; } = string.Empty;

    public Task RunAsync(CancellationToken token) => RunCoreAsync(token);

    private async Task RunCoreAsync(CancellationToken token)
    {
        await encoder.WriteGreetingAsync(token);
        await encoder.WriteCommandAsync("READY\0"u8.ToArray(), token);
        await parser.ParseAsync(Sink ?? throw new InvalidOperationException("sink is not set"), token);
    }

    /// <summary>Resumes this connection after backpressure paused it.</summary>
    public void Resume() => parser.Resume();

    /// <summary>Closes the underlying transport; the parse loop ends with EOF.</summary>
    public void Abort() => transport.Dispose();

    public async ValueTask SendAsync(IZMessage message, CancellationToken token)
    {
        await sendGate.WaitAsync(token);
        try
        {
            await encoder.WriteMessageAsync(message, token);
        }
        finally
        {
            sendGate.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            if (Interlocked.Exchange(ref closed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            parser.Dispose();
            transport.Dispose();
            sendGate.Dispose();
            return ValueTask.CompletedTask;
        }
        catch (Exception exception)
        {
            return ValueTask.FromException(exception);
        }
    }
}
