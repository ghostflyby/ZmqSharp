using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Allocates the final segments for a frame in materialization mode.</summary>
internal delegate ZFrame ZFrameAllocator(int frameLength, bool more);

/// <summary>
/// Pipe-free ZMTP 3.0 traffic parser: frame-header length lookahead and
/// streaming frame delivery. The greeting and mechanism handshake run on
/// socket runtime first; the caller supplies a byte reader, callback and
/// optional frame codec here and calls <see cref="ParseAsync"/> once. A reusable
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

    private readonly IZByteReader reader;
    private readonly ZFrameHandlerAsync onFrame;
    private readonly IZFrameCodec? codec;
    private readonly long maxEncodedLength;
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

    public ZmtpParser(IZByteReader reader, ZFrameHandlerAsync onFrame,
        IZFrameCodec? codec = null, MemoryPool<byte>? pool = null)
        : this(reader, onFrame, null, pool ?? MemoryPool<byte>.Shared, DefaultMaxCommandSize, codec)
    {
    }

    internal ZmtpParser(IZByteReader reader, ZFrameHandlerAsync onFrame,
        ZFrameAllocator? allocator, MemoryPool<byte> pool,
        int maxCommandSize = DefaultMaxCommandSize, IZFrameCodec? codec = null,
        long maxFrameLength = int.MaxValue)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(onFrame);
        this.reader = reader;
        this.onFrame = onFrame;
        this.allocator = allocator;
        this.pool = pool;
        this.maxCommandSize = maxCommandSize;
        this.codec = codec;
        maxEncodedLength = codec?.GetMaximumEncodedLength(Math.Max(maxFrameLength, maxCommandSize)) ?? long.MaxValue;
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
    /// Streams logical frames to the explicit receive callback. The
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

            if (codec is { } transform)
            {
                if (header.Size > maxEncodedLength || header.Size > int.MaxValue)
                    throw new ZeroMqProtocolException("encoded frame exceeds maximum supported length");
                var encodedLength = (int)header.Size;
                EnsureScratchCapacity(encodedLength);
                if (!await TryReadExactlyAsync(scratch[..encodedLength], token)) return;
                var decoded = transform.Decode(new ZmtpFrameData
                {
                    Flags = header.Flags & ~ZmtpFrameFlags.LongSize,
                    Body = new ReadOnlySequence<byte>(scratch[..encodedLength])
                });
                ZmtpFrameEncoder.ValidateFlags(decoded.Flags);
                if ((decoded.Flags & ZmtpFrameFlags.Command) != 0)
                {
                    if (decoded.Body.Length > maxCommandSize)
                        throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
                    if (!decoded.Body.IsSingleSegment)
                        throw new ZeroMqProtocolException("decoded command must be contiguous");
                    CheckCommand(decoded.Body.First.Span);
                    continue;
                }
                var decodedLength = checked((int)decoded.Body.Length);
                var decodedMore = (decoded.Flags & ZmtpFrameFlags.More) != 0;
                ZFrame decodedFrame;
                if (allocator is { } allocate)
                {
                    decodedFrame = allocate(decodedLength, decodedMore);
                    var remaining = decoded.Body;
                    for (var i = 0; i < decodedFrame.Count; i++)
                    {
                        var segment = decodedFrame[i];
                        remaining.Slice(0, segment.Memory.Length).CopyTo(segment.Writable.Span);
                        remaining = remaining.Slice(segment.Memory.Length);
                    }
                }
                else
                {
                    if (!decoded.Body.IsSingleSegment)
                    {
                        EnsureScratchCapacity(decodedLength);
                        decoded.Body.CopyTo(scratch.Span);
                        decodedFrame = new ZFrame(ZSegment.Borrowed(scratch[..decodedLength]), decodedMore);
                    }
                    else decodedFrame = new ZFrame(ZSegment.Borrowed(decoded.Body.First), decodedMore);
                }
                if (!await onFrame(decodedFrame, token)) await WaitForResumeAsync(token);
                continue;
            }

            if ((header.Flags & ZmtpFrameFlags.Command) != 0)
            {
                var commandBody = await ReadBodyIntoScratchAsync(header, token);
                if (commandBody is null) return;

                CheckCommand(commandBody.Value.Span);

                scratchUsed = 0;
                MaybeShrinkScratch();
                continue;
            }

            if (header.Size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

            var length = (int)header.Size;
            var more = (header.Flags & ZmtpFrameFlags.More) != 0;
            if (allocator is not null)
            {
                var materialized = allocator(length, more);
                bool complete;
                try
                {
                    complete = true;
                    for (var i = 0; i < materialized.Count; i++)
                        if (!await TryReadExactlyAsync(materialized[i].Writable, token))
                        {
                            complete = false;
                            break;
                        }
                }
                catch
                {
                    materialized.Dispose();
                    throw;
                }
                if (!complete)
                {
                    materialized.Dispose();
                    return;
                }
                if (!await onFrame(materialized, token)) await WaitForResumeAsync(token);
                continue;
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
            var keepGoing = await onFrame(frame, token);
            if (!keepGoing) await WaitForResumeAsync(token);

            // The borrowed frame must outlive the await; the scratch is
            // reused only after delivery (and any pause) completes.
            scratchUsed = 0;
            MaybeShrinkScratch();
        }
    }

    private static void CheckCommand(ReadOnlySpan<byte> body)
    {
        if (!ZmtpCommandCodec.TryReadCommandName(body, out var name))
            throw new ZeroMqProtocolException("malformed command name");
        if (name.SequenceEqual("ERROR"u8))
            throw new ZeroMqProtocolException($"peer sent ERROR: {ZmtpCommandCodec.ParseErrorReason(body[(1 + name.Length)..])}");
    }

    // ---- Read helpers ----

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await reader.ReadAsync(target[filled..], token);
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

        if ((flags & ZmtpFrameFlags.Command) != 0 && (flags & ZmtpFrameFlags.More) != 0)
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

        var isLong = (flags & ZmtpFrameFlags.LongSize) != 0;
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
