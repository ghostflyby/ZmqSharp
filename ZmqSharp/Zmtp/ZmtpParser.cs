using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Allocates the final segments for a frame in materialization mode.</summary>
internal delegate ZFrameSegments ZFrameAllocator(int frameLength, bool more);

/// <summary>
/// Pipe-free ZMTP 3.0 parser (NULL mechanism): frame-header length lookahead and
/// streaming frame delivery. Call ParseAsync once per connection; a reusable
/// scratch buffer keeps the steady state allocation-free. EOF is treated as
/// connection close (partial data is discarded and never delivered); protocol
/// violations throw ZeroMqProtocolException.
/// </summary>
public sealed class ZmtpParser : IDisposable
{
    private const int InitialScratchSize = 4096;
    private const int ScratchShrinkThreshold = 1 << 20;

    private readonly IZConnection connection;
    private readonly MemoryPool<byte> pool;
    private readonly ZFrameAllocator? allocator;
    private readonly byte[] headerBuffer = new byte[9];

    private IMemoryOwner<byte>? scratchOwner;
    private Memory<byte> scratch;
    private int scratchUsed;

    private readonly Lock gateLock = new();
    private TaskCompletionSource gate = CreateGate();

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ZmtpParser(IZConnection connection, MemoryPool<byte>? pool = null)
        : this(connection, null, pool ?? MemoryPool<byte>.Shared)
    {
    }

    internal ZmtpParser(IZConnection connection, ZFrameAllocator? allocator, MemoryPool<byte> pool)
    {
        ArgumentNullException.ThrowIfNull(connection);
        this.connection = connection;
        this.allocator = allocator;
        this.pool = pool;
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
    /// Completes the greeting and the NULL handshake. Returns false when the peer
    /// closed during establishment.
    /// </summary>
    public async ValueTask<bool> EstablishAsync(CancellationToken token = default)
    {
        if (!await ReadGreetingAsync(token))
        {
            return false;
        }

        if (!await ReadHandshakeAsync(token))
        {
            return false;
        }

        return true;
    }

    /// <summary>
    /// Streams message frames to the connection's receive callbacks; the caller
    /// is responsible for completing EstablishAsync first.
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

    private async ValueTask ReadTrafficAsync(CancellationToken token)
    {
        while (true)
        {
            var nullableHeader = await TryReadFrameHeaderAsync(token);
            if (nullableHeader is not {} header)
            {
                return;
            }

            if (header.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                await ReadBodyIntoScratchAsync(header, token);
                continue;
            }

            if (header.Size > int.MaxValue)
            {
                throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");
            }

            var length = (int)header.Size;
            var more = header.Flags.HasFlag(ZmtpFrameFlags.More);
            if (allocator is not null)
            {
                var segments = allocator(length, more);
                if (segments.Single is { } single)
                {
                    if (!await TryReadExactlyAsync(single.Writable, token))
                    {
                        if (single.Owner is IMemoryOwner<byte> memoryOwner)
                        {
                            memoryOwner.Dispose();
                        }

                        return;
                    }

                    var materialized = new ZFrame(more, segments);
                    var materializedKeepGoing = connection.OnFrame(materialized, token);
                    if (!materializedKeepGoing)
                    {
                        await WaitForResumeAsync(token);
                    }

                    continue;
                }

                if (segments.Many is { } many)
                {
                    foreach (var segment in many)
                    {
                        if (await TryReadExactlyAsync(segment.Writable, token))
                        {
                            continue;
                        }

                        foreach (var owned in many)
                        {
                            if (owned.Owner is IMemoryOwner<byte> memoryOwner)
                            {
                                memoryOwner.Dispose();
                            }
                        }

                        return;
                    }

                    var materializedMulti = new ZFrame(more, segments);
                    var multiKeepGoing = connection.OnFrame(materializedMulti, token);
                    if (!multiKeepGoing)
                    {
                        await WaitForResumeAsync(token);
                    }

                    continue;
                }
            }

            EnsureScratchCapacity(checked(scratchUsed + length));
            var target = scratch.Slice(scratchUsed, length);
            if (!await TryReadExactlyAsync(target, token))
            {
                return;
            }

            var frame = new ZFrame(
                more,
                new ZFrameSegments
                {
                    Single = new ZBufferRef(
                        ZBufferRef.NoopOwner,
                        scratch[scratchUsed..(scratchUsed + length)]),
                });
            var keepGoing = connection.OnFrame(frame, token);
            scratchUsed = 0;
            MaybeShrinkScratch();
            if (!keepGoing)
            {
                await WaitForResumeAsync(token);
            }
        }
    }

    // ---- Read helpers ----

    private async ValueTask<bool> TryReadExactlyAsync(Memory<byte> target, CancellationToken token)
    {
        var filled = 0;
        while (filled < target.Length)
        {
            var count = await connection.ReadAsync(target[filled..], token);
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
