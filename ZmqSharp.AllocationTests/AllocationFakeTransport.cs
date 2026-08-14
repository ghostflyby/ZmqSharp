using System.Buffers.Binary;
using System.Net;
using System.Threading.Channels;
using ZmqSharp.Transports;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Transport whose connection answers the complete ZMTP handshake (greeting +
/// PAIR READY) and then feeds scripted inbound frames, so the receiving pump
/// has real frames to parse and deliver to the semantic seam. Writes are
/// no-ops, so the send hot path completes synchronously on the caller's
/// thread. Tests push frames through <see cref="AllocationFakeConnection.Enqueue"/>
/// (reachable via <see cref="Current"/>).
/// </summary>
internal sealed class AllocationFakeTransport : IZTransport<AllocationFakeTransport, EndPoint>
{
    public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
    {
        add { }
        remove { }
    }

    public static ValueTask<IZConnection> ConnectAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        var connection = new AllocationFakeConnection();
        Current = connection;
        return ValueTask.FromResult<IZConnection>(connection);
    }

    public static ValueTask<AllocationFakeTransport> BindAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        return ValueTask.FromResult(new AllocationFakeTransport());
    }

    public ValueTask StartAsync(CancellationToken token = default)
    {
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
    }

    /// <summary>The connection created by the most recent ConnectAsync. Tests in
    /// this non-parallel project use it to script the peer's inbound frames.</summary>
    public static AllocationFakeConnection? Current { get; private set; }
}

internal sealed class AllocationFakeConnection : IZConnection
{
    private static readonly byte[] Handshake = BuildHandshake();

    private readonly Channel<byte[]> inbound =
        Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Lock gateLock = new();
    private TaskCompletionSource pumpIdle = CreateGate();
    private TaskCompletionSource pumpRelease = CreateCompletedGate();

    private byte[]? current;
    private int currentPosition;
    private int handshakePosition;
    private int disposed;
    private Func<ZFrame, CancellationToken, ValueTask<bool>>? onFrame;

    /// <summary>Feeds one scripted inbound chunk (a complete frame) to the receive pump.</summary>
    public void Enqueue(byte[] chunk)
    {
        inbound.Writer.TryWrite(chunk);
    }

    /// <summary>
    /// Test seam (mock surface): arms a deterministic pumping window. The pump
    /// parks before awaiting an empty channel; with the gate closed it blocks
    /// there, so a test can wait until the pump is parked
    /// (<see cref="WaitUntilPumpIdleAsync"/>), enqueue every measured frame,
    /// and only then release (<see cref="ReleasePump"/>). The pump then drains
    /// the fully-buffered channel in one continuation on one thread - the
    /// thread-local GC window the allocation tests measure (0008). The pump
    /// may already be blocked on the channel read when the test arms (e.g.
    /// after warm-up), so the test wakes it with one sentinel frame before
    /// waiting for it to park.
    /// </summary>
    public void ArmSynchronousWindow()
    {
        lock (gateLock)
        {
            pumpIdle = CreateGate();
            pumpRelease = CreateGate();
        }
    }

    /// <summary>Test seam: completes when the pump is parked at the window gate.</summary>
    public Task WaitUntilPumpIdleAsync()
    {
        lock (gateLock) return pumpIdle.Task;
    }

    /// <summary>Test seam: releases the parked pump; the buffered frames then drain synchronously.</summary>
    public void ReleasePump()
    {
        lock (gateLock) pumpRelease.TrySetResult();
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        if (handshakePosition < Handshake.Length)
        {
            var count = Math.Min(buffer.Length, Handshake.Length - handshakePosition);
            Handshake.AsSpan(handshakePosition, count).CopyTo(buffer.Span);
            handshakePosition += count;
            return ValueTask.FromResult(count);
        }

        return ReadInboundAsync(buffer, token);
    }

    private async ValueTask<int> ReadInboundAsync(Memory<byte> buffer, CancellationToken token)
    {
        while (true)
        {
            if (current is not null && currentPosition < current.Length)
            {
                var count = Math.Min(buffer.Length, current.Length - currentPosition);
                current.AsSpan(currentPosition, count).CopyTo(buffer.Span);
                currentPosition += count;
                return count;
            }

            // Park before awaiting an empty channel: announce the state so a
            // test can wait for it, then block on the release gate (a no-op
            // when the gate is open, as during the handshake and warm-up).
            TaskCompletionSource release;
            lock (gateLock)
            {
                pumpIdle.TrySetResult();
                release = pumpRelease;
            }

            await release.Task;
            current = await inbound.Reader.ReadAsync(token);
            currentPosition = 0;
        }
    }

    private static TaskCompletionSource CreateGate()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static TaskCompletionSource CreateCompletedGate()
    {
        var gate = CreateGate();
        gate.TrySetResult();
        return gate;
    }

    public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        return ValueTask.CompletedTask;
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
    {
        return WriteAsync(frame, token);
    }

    public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        return WriteAsync(body, token);
    }

    public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
    {
        return WriteAsync(ReadOnlyMemory<byte>.Empty, token);
    }

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        return onFrame?.Invoke(frame, token) ?? ValueTask.FromResult(true);
    }

    public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
    {
        this.onFrame = onFrame;
    }

    public void SetConnectionEndedHandler(Action onConnectionEnded)
    {
    }

    public void OnConnectionEnded()
    {
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref disposed, 1);
    }

    /// <summary>ZMTP 3.0 NULL greeting plus a READY command advertising PAIR.</summary>
    private static byte[] BuildHandshake()
    {
        var greeting = new byte[64];
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;
        "NULL"u8.CopyTo(greeting.AsSpan(12, 4));

        // READY body: [5]"READY"[11]"Socket-Type"[int32 BE 4]"PAIR", then a
        // command frame (flags 0b0100, short header).
        var body = new byte[26];
        body[0] = 5;
        "READY"u8.CopyTo(body.AsSpan(1));
        body[6] = 11;
        "Socket-Type"u8.CopyTo(body.AsSpan(7));
        BinaryPrimitives.WriteInt32BigEndian(body.AsSpan(18), 4);
        "PAIR"u8.CopyTo(body.AsSpan(22));

        var frame = new byte[2 + body.Length];
        frame[0] = 0b0100; // ZmtpFrameFlags.Command
        frame[1] = (byte)body.Length;
        body.CopyTo(frame.AsSpan(2));

        return [.. greeting, .. frame];
    }
}

/// <summary>Builds ZMTP 3.0 short-header frame bytes for scripted inbound data.</summary>
internal static class AllocationFrameData
{
    public static byte[] Frame(byte[] body, bool more = false)
    {
        var frame = new byte[2 + body.Length];
        frame[0] = (byte)(more ? 0b0001 : 0b0000); // ZmtpFrameFlags.More
        frame[1] = (byte)body.Length;
        body.CopyTo(frame.AsSpan(2));
        return frame;
    }
}
