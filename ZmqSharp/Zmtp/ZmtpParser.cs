using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;

namespace ZmqSharp.Zmtp;

/// <summary>
/// Pipe-free ZMTP 3.0 parser (NULL mechanism): frame-header length lookahead,
/// policy-driven materialization, and dual-mode delivery. Call ParseAsync once
/// per connection; borrowed mode reuses a scratch buffer with zero steady-state
/// allocation. EOF is treated as connection close (partial data is discarded and
/// never delivered); protocol violations throw ZeroMqProtocolException.
/// </summary>
public sealed class ZmtpParser : IDisposable
{
    private const int SegmentBlockSize = 8192;
    private const int InitialScratchSize = 4096;
    private const int ScratchShrinkThreshold = 1 << 20;

    private readonly Stream stream;
    private readonly ZReceiveOptions options;
    private readonly MemoryPool<byte> pool;
    private readonly byte[] headerBuffer = new byte[9];

    private readonly ZMessageData borrowedData = new();
    private readonly ZSegment borrowedSegment = new()
    {
        Origin = ZBufferOrigin.Pooled,
    };
    private IMemoryOwner<byte>? scratchOwner;
    private Memory<byte> scratch;
    private int scratchUsed;
    private ZMessageData? ownedData;

    private readonly Lock gateLock = new();
    private TaskCompletionSource gate = CreateGate();

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ZmtpParser(Stream stream, ZReceiveOptions options, MemoryPool<byte>? pool = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(options);
        this.stream = stream;
        this.options = options;
        this.pool = pool ?? MemoryPool<byte>.Shared;
    }

    /// <summary>Call after a borrowed callback returns false to resume the receive loop.</summary>
    public void Resume()
    {
        lock (gateLock)
        {
            gate.TrySetResult();
            gate = CreateGate();
        }
    }

    public async ValueTask ParseAsync(IZMessageSink sink, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        if (!await ReadGreetingAsync(token))
        {
            return;
        }

        if (!await ReadHandshakeAsync(token))
        {
            return;
        }

        try
        {
            await ReadTrafficAsync(sink, token);
        }
        finally
        {
            DisposeInFlight();
        }
    }

    public void Dispose()
    {
        scratchOwner?.Dispose();
        scratchOwner = null;
        scratch = Memory<byte>.Empty;
        scratchUsed = 0;
        DisposeInFlight();
    }

    // ---- Greeting ----

    private async ValueTask<bool> ReadGreetingAsync(CancellationToken token)
    {
        using var owner = pool.Rent(64);
        var greeting = owner.Memory[..64];
        if (!await TryReadExactlyAsync(greeting, token))
        {
            return false;
        }

        var span = greeting.Span;
        if (span[0] != 0xFF || span[9] != 0x7F)
        {
            throw new ZeroMqProtocolException("invalid ZMTP greeting signature");
        }

        if (span[10] < 3)
        {
            throw new ZeroMqProtocolException("unsupported ZMTP version");
        }

        if (!IsNullMechanism(span[12..32]))
        {
            throw new ZeroMqProtocolException("unsupported ZMTP security mechanism (only NULL is supported)");
        }

        return true;
    }

    private static bool IsNullMechanism(ReadOnlySpan<byte> mechanism)
    {
        if (mechanism[0] != (byte)'N' || mechanism[1] != (byte)'U' ||
            mechanism[2] != (byte)'L' || mechanism[3] != (byte)'L')
        {
            return false;
        }

        for (var i = 4; i < mechanism.Length; i++)
        {
            if (mechanism[i] != 0)
            {
                return false;
            }
        }

        return true;
    }

    // ---- Handshake (NULL) ----

    private async ValueTask<bool> ReadHandshakeAsync(CancellationToken token)
    {
        while (true)
        {
            var header = await TryReadFrameHeaderAsync(token);
            if (header is null)
            {
                return false;
            }

            if (!header.Value.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                throw new ZeroMqProtocolException("expected a command frame during handshake");
            }

            var body = await ReadBodyIntoScratchAsync(header.Value, token);
            if (body is null)
            {
                return false;
            }

            if (IsCommandName(body.Value, "READY"u8))
            {
                return true;
            }

            if (IsCommandName(body.Value, "ERROR"u8))
            {
                throw new ZeroMqProtocolException($"peer sent ERROR: {ErrorReason(body.Value)}");
            }
        }
    }

    private static bool IsCommandName(ReadOnlyMemory<byte> body, ReadOnlySpan<byte> name)
    {
        var span = body.Span;
        if (span.Length < name.Length + 1 || span[name.Length] != 0)
        {
            return false;
        }

        return span[..name.Length].SequenceEqual(name);
    }

    private static string ErrorReason(ReadOnlyMemory<byte> body)
    {
        var span = body.Span;
        var separator = span.IndexOf((byte)0);
        return separator < 0 ? string.Empty : Encoding.UTF8.GetString(span[(separator + 1)..]);
    }

    // ---- Traffic ----

    private async ValueTask ReadTrafficAsync(IZMessageSink sink, CancellationToken token)
    {
        while (true)
        {
            var header = await TryReadFrameHeaderAsync(token);
            if (header is null)
            {
                return;
            }

            if (header.Value.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                await ReadBodyIntoScratchAsync(header.Value, token);
                continue;
            }

            if (!await ReadMessageAsync(header.Value, sink, token))
            {
                return;
            }
        }
    }

    private async ValueTask<bool> ReadMessageAsync(FrameHeader first, IZMessageSink sink, CancellationToken token)
    {
        var firstContext = new ZReceiveContext(ReadOnlySequence<byte>.Empty, 0, first.Size, 0);
        var firstAction = ResolveAction(sink, firstContext);
        var mode = firstAction.Mode;

        if (mode == ZReceiveMode.Borrowed)
        {
            borrowedData.SetSingleSegment(borrowedSegment);
            scratchUsed = 0;
        }
        else
        {
            ownedData = new ZMessageData();
        }

        var header = first;
        long bytesSeen = 0;
        var framesSeen = 0;
        while (true)
        {
            var action = firstAction;
            if (framesSeen > 0)
            {
                var firstFrame = mode == ZReceiveMode.Borrowed
                    ? borrowedData.GetFrame(0)
                    : (ownedData ?? throw new InvalidOperationException("message data is not initialized")).GetFrame(0);
                var context = new ZReceiveContext(firstFrame, bytesSeen, header.Size, framesSeen);
                var decided = ResolveAction(sink, context);
                action = new ZReceiveAction { Mode = mode, Contiguous = decided.Contiguous };
            }

            if (!await MaterializeFrameAsync(header, action, token))
            {
                DisposeInFlight();
                return false;
            }

            bytesSeen += header.Size;
            framesSeen++;

            if (!header.Flags.HasFlag(ZmtpFrameFlags.More))
            {
                break;
            }

            var next = await TryReadFrameHeaderAsync(token);
            if (next is null)
            {
                DisposeInFlight();
                return false;
            }

            if (next.Value.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                throw new ZeroMqProtocolException("command frame interleaved inside a message");
            }

            header = next.Value;
        }

        await DeliverMessageAsync(sink, mode, token);
        return true;
    }

    private ZReceiveAction ResolveAction(IZMessageSink sink, in ZReceiveContext context)
    {
        var decided = sink.Decide(context) ?? options.Decide?.Invoke(context);
        if (decided is { } action)
        {
            return action;
        }

        var contiguous = options.ContiguousFrameLimit > 0 &&
                         context.NextFrameSize <= options.ContiguousFrameLimit;
        return new ZReceiveAction { Mode = options.Policy, Contiguous = contiguous };
    }

    private async ValueTask<bool> MaterializeFrameAsync(
        FrameHeader header,
        ZReceiveAction action,
        CancellationToken token)
    {
        if (header.Size > int.MaxValue)
        {
            throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");
        }

        var length = (int)header.Size;

        if (action.Mode == ZReceiveMode.Borrowed)
        {
            EnsureScratchCapacity(checked(scratchUsed + length));
            var target = scratch.Slice(scratchUsed, length);
            if (!await TryReadExactlyAsync(target, token))
            {
                return false;
            }

            borrowedData.AddFrame(0, scratchUsed, length);
            scratchUsed += length;
            borrowedSegment.Memory = scratch[..scratchUsed];
            return true;
        }

        var data = ownedData ?? throw new InvalidOperationException("message data is not initialized");
        var contiguous = action.Contiguous && length <= options.ContiguousFrameLimit;
        if (contiguous)
        {
            var segment = await ReadContiguousAsync(length, action.Mode, token);
            if (segment is null)
            {
                return false;
            }

            data.AddSegment(segment);
            data.AddFrame(data.SegmentCount - 1, 0, length);
            return true;
        }

        var firstSegment = data.SegmentCount;
        long remaining = length;
        while (remaining > 0)
        {
            var blockLength = (int)Math.Min(remaining, SegmentBlockSize);
            var segment = await ReadContiguousAsync(blockLength, action.Mode, token);
            if (segment is null)
            {
                return false;
            }

            data.AddSegment(segment);
            remaining -= blockLength;
        }

        data.AddFrame(firstSegment, 0, length);
        return true;
    }

    private async ValueTask<ZSegment?> ReadContiguousAsync(int length, ZReceiveMode mode, CancellationToken token)
    {
        if (mode == ZReceiveMode.Owned)
        {
            var array = GC.AllocateUninitializedArray<byte>(length);
            if (!await TryReadExactlyAsync(array, token))
            {
                return null;
            }

            return new ZSegment { Origin = ZBufferOrigin.Owned, Memory = array };
        }

        var owner = pool.Rent(length);
        if (!await TryReadExactlyAsync(owner.Memory[..length], token))
        {
            owner.Dispose();
            return null;
        }

        return new ZSegment
        {
            Origin = ZBufferOrigin.Pooled,
            Owner = owner,
            Memory = owner.Memory[..length],
        };
    }

    private async ValueTask DeliverMessageAsync(IZMessageSink sink, ZReceiveMode mode, CancellationToken token)
    {
        if (mode == ZReceiveMode.Borrowed)
        {
            borrowedSegment.Memory = scratch[..scratchUsed];
            var view = new ZMessageView(borrowedData);
            var keepGoing = sink.OnBorrowed(view, token);
            borrowedData.Reset();
            scratchUsed = 0;
            MaybeShrinkScratch();
            if (!keepGoing)
            {
                await WaitForResumeAsync(token);
            }

            return;
        }

        var data = ownedData ?? throw new InvalidOperationException("message data is not initialized");
        ownedData = null;
        var message = new ZMessage(data);
        if (!sink.OnOwned(message, token))
        {
            message.Dispose();
        }
    }

    private void DisposeInFlight()
    {
        ownedData?.Dispose();
        ownedData = null;
        borrowedData.Reset();
        scratchUsed = 0;
    }

    // ---- Read helpers ----

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await stream.ReadAsync(target[filled..], token);
            if (count == 0)
            {
                return false;
            }

            filled += count;
        }

        return true;
    }

    private readonly record struct FrameHeader(ZmtpFrameFlags Flags, long Size);

    private async ValueTask<FrameHeader?> TryReadFrameHeaderAsync(CancellationToken token)
    {
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(0, 1), token))
        {
            return null;
        }

        var flags = (ZmtpFrameFlags)headerBuffer[0];
        if ((flags & (ZmtpFrameFlags)0b1111_1000) != 0)
        {
            throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");
        }

        if (flags.HasFlag(ZmtpFrameFlags.Command) && flags.HasFlag(ZmtpFrameFlags.More))
        {
            throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");
        }

        var isLong = flags.HasFlag(ZmtpFrameFlags.LongSize);
        var sizeLength = isLong ? 8 : 1;
        if (!await TryReadExactlyAsync(headerBuffer.AsMemory(1, sizeLength), token))
        {
            return null;
        }

        var size = isLong
            ? BinaryPrimitives.ReadInt64BigEndian(headerBuffer.AsSpan(1, 8))
            : headerBuffer[1];
        if (size < 0)
        {
            throw new ZeroMqProtocolException("negative ZMTP frame size");
        }

        return new FrameHeader(flags, size);
    }

    private async ValueTask<ReadOnlyMemory<byte>?> ReadBodyIntoScratchAsync(
        FrameHeader header,
        CancellationToken token)
    {
        if (header.Size > int.MaxValue)
        {
            throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");
        }

        var length = (int)header.Size;
        EnsureScratchCapacity(checked(scratchUsed + length));
        var target = scratch.Slice(scratchUsed, length);
        if (!await TryReadExactlyAsync(target, token))
        {
            return null;
        }

        var body = scratch.Slice(scratchUsed, length);
        scratchUsed += length;
        return body;
    }

    private void EnsureScratchCapacity(int required)
    {
        if (scratch.Length >= required)
        {
            return;
        }

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
