using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Allocates the final segments for a frame in materialization mode.</summary>
internal delegate ZFrame ZFrameAllocator(int frameLength, bool more);

/// <summary>
/// Pipe-free ZMTP 3.0 traffic parser: frame-header length lookahead and
/// streaming frame delivery. The greeting and mechanism handshake run on
/// <see cref="ZmtpHandshake"/> first; the caller passes the handshake's
/// session connection here and calls <see cref="ParseAsync"/> once. A reusable
/// scratch buffer keeps the steady state allocation-free. EOF is treated as
/// connection close (partial data is discarded and never delivered); protocol
/// violations throw ZeroMqProtocolException.
/// </summary>
public sealed class ZmtpParser : IDisposable
{
    private const int InitialScratchSize = 4096;
    private const int ScratchShrinkThreshold = 1 << 20;

    /// <summary>Default command-size limit (0008 Slice B).</summary>
    public const int DefaultMaxCommandSize = 1 << 20;

    private readonly IZConnection connection;
    private readonly MemoryPool<byte> pool;
    private readonly ZFrameAllocator? allocator;
    private readonly int maxCommandSize;
    private readonly byte[] headerBuffer = new byte[9];

    private IMemoryOwner<byte>? scratchOwner;
    private Memory<byte> scratch;
    private int scratchUsed;

    private readonly Lock gateLock = new();
    private TaskCompletionSource gate = CreateGate();

    private static TaskCompletionSource CreateGate()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public ZmtpParser(IZConnection connection, MemoryPool<byte>? pool = null)
        : this(connection, null, pool ?? MemoryPool<byte>.Shared)
    {
    }


    internal ZmtpParser(
        IZConnection connection,
        ZFrameAllocator? allocator,
        MemoryPool<byte> pool,
        int maxCommandSize = DefaultMaxCommandSize)
    {
        ArgumentNullException.ThrowIfNull(connection);
        this.connection = connection;
        this.allocator = allocator;
        this.pool = pool;
        this.maxCommandSize = maxCommandSize;
    }

    /// <summary>Call after a streaming callback returns false to resume the receive loop.</summary>
    public void Resume()
    {
        lock (gateLock)
        {
            gate.TrySetResult();
            gate = CreateGate();
        }
    }

    /// <summary>
    /// Streams message frames to the connection's receive callbacks. The
    /// caller is responsible for completing the handshake first: the
    /// connection must already be established, and traffic frames must not
    /// precede the mechanism's READY, or the READY is delivered as a
    /// malformed-command error.
    /// </summary>
    public async ValueTask ParseAsync(CancellationToken token = default)
    {
        await ReadTrafficAsync(token);
    }

    public void Dispose()
    {
        scratchOwner?.Dispose();
        scratchOwner = null;
        scratch = Memory<byte>.Empty;
        scratchUsed = 0;
    }

    // ---- Traffic ----

    private async ValueTask ReadTrafficAsync(CancellationToken token)
    {
        while (true)
        {
            var nullableHeader = await TryReadFrameHeaderAsync(token);
            if (nullableHeader is not { } header) return;

            if (header.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                var commandBody = await ReadBodyIntoScratchAsync(header, token);
                if (commandBody is null) return;

                if (!ZmtpCommandCodec.TryReadCommandName(commandBody.Value.Span, out var commandName))
                    throw new ZeroMqProtocolException("malformed command name");

                if (commandName.SequenceEqual("ERROR"u8))
                    throw new ZeroMqProtocolException(
                        $"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(commandBody.Value.Span[(1 + commandName.Length)..])}");

                scratchUsed = 0;
                MaybeShrinkScratch();
                continue;
            }

            if (header.Size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

            var length = (int)header.Size;
            var more = header.Flags.HasFlag(ZmtpFrameFlags.More);
            if (allocator is not null)
            {
                var materialized = allocator(length, more);
                if (materialized.TryGetValue(out ZSegment single))
                {
                    if (!await TryReadExactlyAsync(single.Writable, token))
                    {
                        single.Dispose();
                        return;
                    }

                    var materializedKeepGoing = await connection.OnFrameAsync(materialized, token);
                    if (!materializedKeepGoing) await WaitForResumeAsync(token);

                    continue;
                }

                if (materialized.TryGetValue(out ZSegments many))
                {
                    for (var i = 0; i < many.Count; i++)
                    {
                        var segment = many[i];
                        if (await TryReadExactlyAsync(segment.Writable, token)) continue;

                        many.Dispose();
                        return;
                    }

                    var multiKeepGoing = await connection.OnFrameAsync(materialized, token);
                    if (!multiKeepGoing) await WaitForResumeAsync(token);

                    continue;
                }
            }

            EnsureScratchCapacity(checked(scratchUsed + length));
            var target = scratch.Slice(scratchUsed, length);
            if (!await TryReadExactlyAsync(target, token)) return;

            // The borrowed segment refers to the scratch owner without taking
            // ownership; EnsureScratchCapacity guarantees the owner is live for
            // the duration of this frame's delivery (0006 3.4).
            if (scratchOwner is not { } source)
                throw new InvalidOperationException("borrowed frame without scratch owner");

            var frame = new ZFrame(ZSegment.Borrowed(source, scratchUsed, length), more);
            var keepGoing = await connection.OnFrameAsync(frame, token);
            if (!keepGoing) await WaitForResumeAsync(token);

            // The borrowed frame must outlive the await; the scratch is
            // reused only after delivery (and any pause) completes.
            scratchUsed = 0;
            MaybeShrinkScratch();
        }
    }

    // ---- Read helpers ----

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await connection.ReadAsync(target[filled..], token);
            if (count == 0) return false;

            filled += count;
        }

        return true;
    }

    private readonly record struct FrameHeader(ZmtpFrameFlags Flags, long Size);

    private async ValueTask<FrameHeader?> TryReadFrameHeaderAsync(CancellationToken token)
    {
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(0, 1), token)) return null;

        var flags = (ZmtpFrameFlags)headerBuffer[0];
        if ((flags & (ZmtpFrameFlags)0b1111_1000) != 0)
            throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

        if (flags.HasFlag(ZmtpFrameFlags.Command) && flags.HasFlag(ZmtpFrameFlags.More))
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var isLong = flags.HasFlag(ZmtpFrameFlags.LongSize);
        var sizeLength = isLong ? 8 : 1;
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(1, sizeLength), token)) return null;

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(headerBuffer.AsSpan(1, 8))
            : headerBuffer[1];
        if (size < 0) throw new ZeroMqProtocolException("negative ZMTP frame size");

        return new FrameHeader(flags, size);
    }

    private async ValueTask<ReadOnlyMemory<byte>?> ReadBodyIntoScratchAsync(
        FrameHeader header,
        CancellationToken token)
    {
        if (header.Size > maxCommandSize)
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");

        if (header.Size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

        var length = (int)header.Size;
        EnsureScratchCapacity(checked(scratchUsed + length));
        var target = scratch.Slice(scratchUsed, length);
        if (!await TryReadExactlyAsync(target, token)) return null;

        var body = scratch.Slice(scratchUsed, length);
        scratchUsed += length;
        return body;
    }

    private void EnsureScratchCapacity(int required)
    {
        // A missing owner means the scratch was never rented (a zero-length
        // first frame, or a shrink); rent before handing out any borrowed
        // frame, since the borrowed branch requires a live owner.
        if (scratch.Length >= required && scratchOwner is not null) return;

        var newSize = Math.Max(required, Math.Max(InitialScratchSize, scratch.Length * 2));
        var newOwner = pool.Rent(newSize);
        var newScratch = newOwner.Memory;
        scratch[..scratchUsed].CopyTo(newScratch);
        scratchOwner?.Dispose();
        scratchOwner = newOwner;
        scratch = newScratch;
    }

    private void MaybeShrinkScratch()
    {
        if (scratch.Length > ScratchShrinkThreshold)
        {
            scratchOwner?.Dispose();
            scratchOwner = null;
            scratch = Memory<byte>.Empty;
        }
    }

    private async ValueTask WaitForResumeAsync(CancellationToken token)
    {
        Task task;
        lock (gateLock)
        {
            task = gate.Task;
        }

        await task.WaitAsync(token);
    }
}
