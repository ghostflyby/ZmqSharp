using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests;

/// <summary>
/// The endpoint transports the real-socket test suites run over (0015 section
/// 5.4). Tcp and Ipc (Unix domain sockets) both run on every platform:
/// ZmqSharp's ipc is real AF_UNIX on Windows 10 1803+ as well (0020). Public
/// because theory methods take it as a parameter.
/// </summary>
public enum TransportKind
{
    Tcp,
    Ipc
}

internal static class TestTransports
{
    /// <summary>
    /// Theory data for transport parameterization: both transports run on
    /// every platform (ipc is real AF_UNIX on Windows too, 0020).
    /// </summary>
    public static TheoryData<TransportKind> TransportKinds()
    {
        return [TransportKind.Tcp, TransportKind.Ipc];
    }

    /// <summary>Builds a fresh string endpoint for the transport kind.</summary>
    public static string GetEndpoint(TransportKind kind)
    {
        return kind == TransportKind.Ipc
            ? $"ipc://{IpcSocketPath("zmqsharp-test-")}"
            : $"tcp://127.0.0.1:{GetFreePort()}";
    }

    /// <summary>
    /// Fresh Unix domain socket paths for the ipc-specific lifecycle tests;
    /// discovered on every platform (Windows AF_UNIX is real, 0020).
    /// </summary>
    public static TheoryData<string> IpcPaths()
    {
        return [IpcSocketPath("zmqsharp-test-")];
    }

    /// <summary>
    /// A fresh Unix domain socket path under the system temp directory.
    /// The filename stays short: macOS limits sun_path to 104 bytes, and long
    /// temp directories plus a 32-char guid would exceed it.
    /// </summary>
    public static string IpcSocketPath(string prefix)
    {
        var id = Guid.NewGuid().ToString("N")[..12];
        return Path.Combine(Path.GetTempPath(), $"{prefix}{id}.sock");
    }

    /// <summary>
    /// Names for the Linux abstract-namespace tests (ipc://@name, 0020). The
    /// data source stays non-empty on every platform (an empty TheoryData
    /// fails discovery); the tests skip at runtime off Linux, where the
    /// abstract namespace does not exist.
    /// </summary>
    public static TheoryData<string> AbstractNames()
    {
        return [$"zmqsharp-abs-{Guid.NewGuid().ToString("N")[..8]}"];
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>In-memory stream; caps the chunk size to simulate partial TCP reads.
/// Writes are captured (appended) so the handshake's local greeting/READY can
/// be written into the connection during tests.</summary>
internal sealed class ChunkedMemoryStream(byte[] data, int maxChunkSize = 0) : Stream
{
    private int position;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => data.Length;

    public override long Position
    {
        get => position;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadInternal(buffer.AsSpan(offset, count));
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(ReadInternal(buffer.Span));
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        // Writes (the local handshake) are captured and never read back.
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return ValueTask.CompletedTask;
    }

    private int ReadInternal(Span<byte> buffer)
    {
        if (position >= data.Length) return 0;

        var available = data.Length - position;
        var chunk = maxChunkSize > 0 ? Math.Min(maxChunkSize, available) : available;
        var count = Math.Min(buffer.Length, chunk);
        data.AsSpan(position, count).CopyTo(buffer);
        position += count;
        return count;
    }
}

/// <summary>Pool that tracks outstanding rentals, used to assert buffers are returned after message Dispose.</summary>
internal sealed class CountingMemoryPool : MemoryPool<byte>
{
    private readonly MemoryPool<byte> inner = Shared;
    private int outstanding;

    public int Outstanding => Volatile.Read(ref outstanding);

    public override int MaxBufferSize => inner.MaxBufferSize;

    public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
    {
        var owner = inner.Rent(minimumBufferSize);
        Interlocked.Increment(ref outstanding);
        return new TrackingOwner(this, owner);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) inner.Dispose();
    }

    private sealed class TrackingOwner(CountingMemoryPool pool, IMemoryOwner<byte> inner) : IMemoryOwner<byte>
    {
        private int disposed;

        public Memory<byte> Memory => inner.Memory;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                Interlocked.Decrement(ref pool.outstanding);
                inner.Dispose();
            }
        }
    }
}

/// <summary>
/// Pool that probes the receive path: counts every Rent and can be armed to
/// throw from Rent, proving whether an over-limit frame was allocated.
/// </summary>
internal sealed class ProbingMemoryPool : MemoryPool<byte>
{
    private readonly MemoryPool<byte> inner = Shared;
    private int rented;

    /// <summary>Total Rent calls since construction or the last <see cref="Reset"/>.</summary>
    public int Rentals => Volatile.Read(ref rented);

    /// <summary>When set, Rent throws before allocating anything.</summary>
    public Exception? FailOnRent { get; set; }

    public override int MaxBufferSize => inner.MaxBufferSize;

    public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
    {
        Interlocked.Increment(ref rented);
        if (FailOnRent is { } failure) throw failure;

        return inner.Rent(minimumBufferSize);
    }

    public void Reset()
    {
        Interlocked.Exchange(ref rented, 0);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) inner.Dispose();
    }
}

/// <summary>Test factory for building ZMessage instances directly.</summary>
internal static class MessageFactory
{
    public static ZMessage SingleFrame(byte[] payload)
    {
        return new ZMessage(new ZSingleMessage(new ZFrame(new ZSegment(payload, 0, payload.Length))));
    }

    public static ZMessage PooledSingleFrame(MemoryPool<byte> pool, byte[] payload)
    {
        var owner = pool.Rent(payload.Length);
        payload.CopyTo(owner.Memory);
        return new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegment(owner, 0, payload.Length))));
    }

    public static ZMessage SegmentedFrame(params byte[][] segments)
    {
        if (segments.Length == 1) return SingleFrame(segments[0]);

        return new ZMessage(new ZSingleMessage(
            new ZFrame(new ZSegments(
                [.. segments.Select(segment => new ZSegment(segment, 0, segment.Length))]))));
    }

    public static ZMessage Multipart(params byte[][] frames)
    {
        return new ZMessage(new ZMultiMessage(
            [.. frames.Select(frame => new ZFrame(new ZSegment(frame, 0, frame.Length)))]));
    }

    public static ZMessage PooledMultipart(MemoryPool<byte> pool, params byte[][] frames)
    {
        var refs = new List<ZFrame>(frames.Length);
        foreach (var frame in frames)
        {
            var owner = pool.Rent(frame.Length);
            frame.CopyTo(owner.Memory);
            refs.Add(new ZFrame(new ZSegment(owner, 0, frame.Length)));
        }

        return new ZMessage(new ZMultiMessage([.. refs]));
    }
}

/// <summary>Captures streamed frames (copied, since frames are borrowed).</summary>
internal sealed class FrameRecorder(Func<ZFrame, CancellationToken, bool>? onFrame = null) : IZMessageSink
{
    public List<byte[]> Frames { get; } = [];

    public List<bool> MoreFlags { get; } = [];

    public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
    {
        frame.TryGetValue(out ZSegment segment);
        Frames.Add(segment.Memory.ToArray());
        MoreFlags.Add(frame.More);
        return ValueTask.FromResult(onFrame?.Invoke(frame, token) ?? true);
    }

    public void OnConnectionEnded()
    {
    }
}

internal static class ZmtpTestRunner
{
    /// <summary>
    /// Completes the NULL handshake (greeting + READY) on the connection and
    /// returns the session connection to run the traffic parser on.
    /// </summary>
    public static async Task<IZConnection?> EstablishAsync(IZConnection connection, string socketType = "PAIR")
    {
        using var handshake = new ZmtpHandshake(
            connection,
            ZNullMechanism.Instance,
            ZmtpCommands.BuildReady(socketType),
            ZmtpParser.DefaultMaxCommandSize);
        var result = await handshake.EstablishAsync(ZMechanismRole.Client);
        return result is { } r ? r.SessionConnection : null;
    }

    public static ZmtpParser CreateParser(IZConnection connection, IZMessageSink sink)
    {
        connection.SetFrameHandler(sink.OnFrameAsync);
        return new ZmtpParser(connection);
    }

    /// <summary>Completes the NULL handshake on the connection, then streams its traffic frames to the sink.</summary>
    public static async Task RunParserAsync(IZConnection connection, IZMessageSink sink)
    {
        var session = await EstablishAsync(connection);
        if (session is null) return;

        using var parser = CreateParser(session, sink);
        await parser.ParseAsync();
    }

    /// <summary>Runs an already-configured parser to completion.</summary>
    public static async Task RunParserAsync(ZmtpParser parser)
    {
        await parser.ParseAsync();
    }
}

/// <summary>ZMTP wire encoding helpers (tests only).</summary>
internal static class ZmtpTestData
{
    public static byte[] Greeting(string mechanism = "NULL")
    {
        var result = new byte[64];
        result[0] = 0xFF;
        result[9] = 0x7F;
        result[10] = 3;
        Encoding.ASCII.GetBytes(mechanism).AsSpan().CopyTo(result.AsSpan(12));
        return result;
    }

    public static byte[] Ready(string socketType = "PAIR")
    {
        return Frame(ReadyBody(socketType), command: true);
    }

    private static byte[] ReadyBody(string socketType = "PAIR")
    {
        return ReadyBodyWithProperties(("Socket-Type", socketType));
    }

    public static byte[] ReadyWithProperties(params (string Name, string Value)[] properties)
    {
        return Frame(ReadyBodyWithProperties(properties), command: true);
    }

    public static byte[] ReadyBodyWithProperties(params (string Name, string Value)[] properties)
    {
        var body = new List<byte> { 5 };
        body.AddRange("READY"u8);
        foreach (var (name, value) in properties)
        {
            var nameBytes = Encoding.ASCII.GetBytes(name);
            var valueBytes = Encoding.UTF8.GetBytes(value);
            body.Add((byte)nameBytes.Length);
            body.AddRange(nameBytes);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(lengthBytes, valueBytes.Length);
            body.AddRange(lengthBytes);
            body.AddRange(valueBytes);
        }

        return [.. body];
    }

    public static byte[] ReadyWithRawProperty(ReadOnlySpan<byte> name, int valueLength)
    {
        return Frame(ReadyBodyWithRawProperty(name, valueLength), command: true);
    }

    private static byte[] ReadyBodyWithRawProperty(ReadOnlySpan<byte> name, int valueLength)
    {
        var body = new byte[6 + 1 + name.Length + 4];
        body[0] = 5;
        "READY"u8.CopyTo(body.AsSpan(1));
        var offset = 6;
        body[offset] = (byte)name.Length;
        offset++;
        name.CopyTo(body.AsSpan(offset));
        offset += name.Length;
        BinaryPrimitives.WriteInt32BigEndian(body.AsSpan(offset), valueLength);
        return body;
    }

    public static byte[] Error(string reason)
    {
        var bytes = Encoding.ASCII.GetBytes(reason);
        var body = new byte[1 + 5 + 1 + bytes.Length];
        body[0] = 5;
        "ERROR"u8.CopyTo(body.AsSpan(1));
        body[6] = (byte)bytes.Length;
        bytes.CopyTo(body.AsSpan(7));
        return Frame(body, command: true);
    }

    public static byte[] Frame(byte[] body, bool more = false, bool command = false, byte flagsOverride = 0)
    {
        var isLong = body.Length > 255;
        var flags = (byte)(
            (more ? ZmtpFrameFlags.More : ZmtpFrameFlags.None)
            | (isLong ? ZmtpFrameFlags.LongSize : ZmtpFrameFlags.None)
            | (command ? ZmtpFrameFlags.Command : ZmtpFrameFlags.None)
            | (ZmtpFrameFlags)flagsOverride);
        if (!isLong)
        {
            var result = new byte[2 + body.Length];
            result[0] = flags;
            result[1] = (byte)body.Length;
            body.CopyTo(result.AsSpan(2));
            return result;
        }

        var longResult = new byte[9 + body.Length];
        longResult[0] = flags;
        BinaryPrimitives.WriteInt64BigEndian(longResult.AsSpan(1, 8), body.Length);
        body.CopyTo(longResult.AsSpan(9));
        return longResult;
    }

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(part => part.Length)];
        var offset = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result.AsSpan(offset));
            offset += part.Length;
        }

        return result;
    }
}

/// <summary>
/// Transport whose connection answers the complete ZMTP handshake (greeting +
/// PAIR READY) and then parks on the read, so the pump stays alive with the
/// peer established - used for allocation measurements of the send path.
/// </summary>
internal sealed class EstablishedFakeTransport : IZTransport<EstablishedFakeTransport, EndPoint>
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
        return ValueTask.FromResult<IZConnection>(new EstablishedFakeConnection());
    }

    public static ValueTask<EstablishedFakeTransport> BindAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        return ValueTask.FromResult(new EstablishedFakeTransport());
    }

    public ValueTask StartAsync(CancellationToken token = default)
    {
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
    }
}

internal sealed class EstablishedFakeConnection : IZConnection
{
    private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"));
    private int position;
    private int disposed;

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        if (position < handshake.Length)
        {
            var count = Math.Min(buffer.Length, handshake.Length - position);
            handshake.AsSpan(position, count).CopyTo(buffer.Span);
            position += count;
            return ValueTask.FromResult(count);
        }

        // Park the pump on a read that only cancellation completes, so the
        // peer stays established and routable for the duration of the test.
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        token.Register(static state => (state as TaskCompletionSource<int>)?.TrySetCanceled(), tcs);
        return new ValueTask<int>(tcs.Task);
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
        return ValueTask.FromResult(true);
    }

    public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
    {
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
}
