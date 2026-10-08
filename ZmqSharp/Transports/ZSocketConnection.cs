using System.Buffers;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace ZmqSharp.Transports;

/// <summary>Raw socket byte I/O, including complete scatter/gather writes.</summary>
internal sealed class ZSocketConnection(Socket socket) : IZConnection
{
    private readonly List<ArraySegment<byte>> segments = [];
    private int aborted;

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        => socket.ReceiveAsync(destination, SocketFlags.None, token);

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
    {
        while (!bytes.IsEmpty)
        {
            var written = await socket.SendAsync(bytes, SocketFlags.None, token);
            if (written == 0) throw new IOException("socket closed during write");
            bytes = bytes[written..];
        }
    }

    public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        if (bytes.IsSingleSegment)
        {
            await WriteAsync(bytes.First, token);
            return;
        }

        segments.Clear();
        foreach (var memory in bytes)
        {
            if (memory.IsEmpty) continue;
            if (MemoryMarshal.TryGetArray(memory, out var segment))
            {
                segments.Add(segment);
                continue;
            }

            var buffer = ArrayPool<byte>.Shared.Rent(checked((int)bytes.Length));
            try
            {
                bytes.CopyTo(buffer);
                await WriteAsync(buffer.AsMemory(0, (int)bytes.Length), token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return;
        }

        // The scatter overload has no token. Aborting the socket cancels the
        // actual operation, so borrowed buffers remain live until it returns.
        using var registration = token.UnsafeRegister(static state =>
        {
            if (state is ZSocketConnection connection) connection.Abort();
        }, this);
        while (segments.Count > 0)
        {
            var written = await socket.SendAsync(segments, SocketFlags.None);
            if (written == 0) throw new IOException("socket closed during scatter write");
            while (written > 0)
            {
                var first = segments[0];
                if (written >= first.Count)
                {
                    written -= first.Count;
                    segments.RemoveAt(0);
                }
                else
                {
                    if (first.Array is not { } array) throw new IOException("scatter segment has no backing array");
                    segments[0] = new ArraySegment<byte>(array, first.Offset + written, first.Count - written);
                    written = 0;
                }
            }
        }
    }

    public void Abort()
    {
        if (Interlocked.Exchange(ref aborted, 1) == 0) socket.Dispose();
    }

    public void Dispose() => Abort();
}
