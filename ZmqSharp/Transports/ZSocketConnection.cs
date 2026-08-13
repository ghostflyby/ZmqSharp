using System.Buffers;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// Concrete full-duplex connection over a raw socket (0015 section 4): reads
/// directly with <see cref="Socket.ReceiveAsync"/> and writes frames with
/// buffer-list scatter sends, removing the <see cref="NetworkStream"/> wrapper
/// and the Stream virtual-call layer from the hot path. The win is not fewer
/// reads (the parser's read-exactly loop is unchanged); it is the wrapper
/// removal plus one system call per frame. The per-connection write gate is
/// retained: message-level atomic writes still require serialization.
/// <see cref="ZConnection"/> stays for generic (non-socket) transports.
/// </summary>
internal sealed class ZSocketConnection : IZConnection
{
    private readonly Socket socket;
    private readonly SocketWriteSink sink;
    private readonly ZmtpFrameEncoder encoder;
    private readonly SemaphoreSlim writeGate = new(1, 1);
    private Func<ZFrame, CancellationToken, ValueTask<bool>>? onFrame;
    private Action? onConnectionEnded;
    private int disposed;

    public ZSocketConnection(Socket socket)
    {
        ArgumentNullException.ThrowIfNull(socket);
        this.socket = socket;
        sink = new SocketWriteSink(socket);
        encoder = new ZmtpFrameEncoder(sink);
    }

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
        return socket.ReceiveAsync(buffer, SocketFlags.None, token);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        await writeGate.WaitAsync(token);
        try
        {
            await sink.WriteAsync(bytes, token);
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

        // Disposing the socket aborts pending async receives/sends, so a
        // pump parked on ReadAsync is released (the DisconnectAsync scenario
        // that a stream dispose could not reliably interrupt, 0006 3.6).
        writeGate.Dispose();
        socket.Dispose();
    }

    /// <summary>
    /// Raw transport write side of the connection: single-system-call writes,
    /// scatter for multi-segment sequences. Ungated by design: the write gate
    /// lives on the connection and is already held by every caller (the
    /// encoder always runs under it).
    /// </summary>
    private sealed class SocketWriteSink(Socket socket) : IZWriteSink
    {
        private readonly List<ArraySegment<byte>> segments = [];

        public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            // ValueTask<int> is not ValueTask; awaiting a synchronously
            // completing send completes this method synchronously (no box in
            // Release), so the raw single-buffer path stays allocation-free.
            await socket.SendAsync(bytes, SocketFlags.None, token);
        }

        public async ValueTask WriteAsync(ReadOnlySequence<byte> sequence, CancellationToken token = default)
        {
            if (sequence.IsSingleSegment)
            {
                await socket.SendAsync(sequence.First, SocketFlags.None, token);
                return;
            }

            // Scatter write: gather the frame's segments into the reusable
            // buffer list and send once (0015 section 6.1). Frame segments
            // are array-backed (header + owned/pooled segment memories), so
            // this path is the rule; non-array-backed (native) memory falls
            // back to a single pooled copy.
            var segments = this.segments;
            segments.Clear();
            foreach (var memory in sequence)
            {
                if (memory.IsEmpty) continue;

                if (MemoryMarshal.TryGetArray(memory, out var arraySegment))
                {
                    segments.Add(arraySegment);
                    continue;
                }

                segments.Clear();
                var owner = ArrayPool<byte>.Shared.Rent((int)sequence.Length);
                try
                {
                    sequence.CopyTo(owner);
                    await socket.SendAsync(owner.AsMemory(0, (int)sequence.Length), SocketFlags.None, token);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(owner);
                }

                return;
            }

            // The scatter overload has no CancellationToken parameter: it is
            // the .NET 4.5-era SocketTaskExtensions surface over
            // SocketAsyncEventArgs, which has no cancellation concept - a
            // submitted SAEA operation can only be aborted by closing the
            // socket. The write gate serializes sends, which is also what
            // keeps the socket's cached TaskSocketAsyncEventArgs (one per
            // socket, exchanged not allocated) reusable. See 0021 section 4.
            if (segments.Count > 0) await socket.SendAsync(segments, SocketFlags.None);
        }
    }
}
