using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>Allocates the final segments for a frame in materialization mode.</summary>
internal delegate ZFrame ZFrameAllocator(int frameLength, bool more);

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
    private string? peerSocketType;

    private readonly Lock gateLock = new();
    private TaskCompletionSource gate = CreateGate();

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Socket-Type advertised by the peer in READY; null before the handshake completes.</summary>
    internal string? PeerSocketType => peerSocketType;

    public ZmtpParser(IZConnection connection, MemoryPool<byte>? pool = null)
        : this(connection, null, pool ?? MemoryPool<byte>.Shared, DefaultMaxCommandSize)
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
            // Greeting revision 0 = ZMTP 1.0, revision 1 = ZMTP 2.0. The whole
            // maintained ZeroMQ ecosystem is on ZMTP 3.0/3.1, so legacy peers
            // are rejected explicitly rather than negotiated down (libzmq
            // itself only keeps the legacy paths for backward compatibility).
            throw new ZeroMqProtocolException(span[10] switch
            {
                0 => "ZMTP 1.0 peers are not supported; only ZMTP 3.0 is implemented",
                1 => "ZMTP 2.0 peers are not supported; only ZMTP 3.0 is implemented",
                _ => "unsupported ZMTP version",
            });
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

            var bodySpan = body.Value.Span;
            if (!TryReadCommandName(bodySpan, out var commandName))
            {
                throw new ZeroMqProtocolException("malformed command name");
            }

            if (commandName.SequenceEqual("READY"u8))
            {
                var properties = ParseMetadata(bodySpan[(1 + commandName.Length)..]);
                if (!properties.TryGetValue("Socket-Type", out var peerType) || !IsValidSocketType(peerType))
                {
                    throw new ZeroMqProtocolException("READY is missing a valid Socket-Type property");
                }

                peerSocketType = peerType;
                scratchUsed = 0;
                MaybeShrinkScratch();
                return true;
            }

            if (commandName.SequenceEqual("ERROR"u8))
            {
                throw new ZeroMqProtocolException(
                    $"peer sent ERROR: {ParseErrorReason(bodySpan[(1 + commandName.Length)..])}");
            }

            scratchUsed = 0;
            throw new ZeroMqProtocolException($"unknown command '{Encoding.ASCII.GetString(commandName)}' during handshake");
        }
    }

    private static bool TryReadCommandName(ReadOnlySpan<byte> body, out ReadOnlySpan<byte> name)
    {
        if (body.IsEmpty || body[0] == 0)
        {
            name = default;
            return false;
        }

        var nameLength = body[0];
        if (body.Length < nameLength + 1)
        {
            name = default;
            return false;
        }

        var candidate = body.Slice(1, nameLength);
        foreach (var c in candidate)
        {
            var isAlpha = (c >= (byte)'A' && c <= (byte)'Z') || (c >= (byte)'a' && c <= (byte)'z');
            if (!isAlpha)
            {
                name = default;
                return false;
            }
        }

        name = candidate;
        return true;
    }

    private static string ParseErrorReason(ReadOnlySpan<byte> body)
    {
        if (body.IsEmpty)
        {
            throw new ZeroMqProtocolException("malformed ERROR command");
        }

        var reasonLength = body[0];
        if (body.Length != 1 + reasonLength)
        {
            throw new ZeroMqProtocolException("ERROR reason length does not match the command body");
        }

        foreach (var c in body[1..])
        {
            if (c is < (byte)0x21 or > (byte)0x7E)
            {
                throw new ZeroMqProtocolException("ERROR reason contains a non-visible character");
            }
        }

        return Encoding.UTF8.GetString(body[1..]);
    }

    private static Dictionary<string, string> ParseMetadata(ReadOnlySpan<byte> metadata)
    {
        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var offset = 0;
        while (offset < metadata.Length)
        {
            var nameLength = metadata[offset];
            offset++;
            if (nameLength == 0)
            {
                throw new ZeroMqProtocolException("metadata property name is empty");
            }

            if (metadata.Length - offset < nameLength)
            {
                throw new ZeroMqProtocolException("metadata property name exceeds command body");
            }

            var name = metadata.Slice(offset, nameLength);
            foreach (var c in name)
            {
                if (!IsMetadataNameChar(c))
                {
                    throw new ZeroMqProtocolException("metadata property name contains an invalid character");
                }
            }

            offset += nameLength;
            if (metadata.Length - offset < sizeof(int))
            {
                throw new ZeroMqProtocolException("metadata property value length is truncated");
            }

            var valueLength = BinaryPrimitives.ReadInt32BigEndian(metadata[offset..]);
            offset += sizeof(int);
            if (valueLength < 0 || valueLength > metadata.Length - offset)
            {
                throw new ZeroMqProtocolException("metadata property value exceeds command body");
            }

            var nameString = Encoding.ASCII.GetString(name);
            var value = Encoding.UTF8.GetString(metadata.Slice(offset, valueLength));
            offset += valueLength;
            if (!properties.TryAdd(nameString, value))
            {
                throw new ZeroMqProtocolException($"duplicate metadata property '{nameString}'");
            }
        }

        return properties;
    }

    private static bool IsMetadataNameChar(byte c)
        => (c >= (byte)'A' && c <= (byte)'Z')
            || (c >= (byte)'a' && c <= (byte)'z')
            || (c >= (byte)'0' && c <= (byte)'9')
            || c == (byte)'-'
            || c == (byte)'_'
            || c == (byte)'.'
            || c == (byte)'+';

    private static bool IsValidSocketType(string socketType) => socketType is
        "REQ" or "REP" or "DEALER" or "ROUTER" or "PUB" or "XPUB" or "SUB" or "XSUB" or "PUSH" or "PULL" or "PAIR";

    // ---- Traffic ----

    private async ValueTask ReadTrafficAsync(CancellationToken token)
    {
        while (true)
        {
            var nullableHeader = await TryReadFrameHeaderAsync(token);
            if (nullableHeader is not { } header)
            {
                return;
            }

            if (header.Flags.HasFlag(ZmtpFrameFlags.Command))
            {
                var commandBody = await ReadBodyIntoScratchAsync(header, token);
                if (commandBody is null)
                {
                    return;
                }

                if (!TryReadCommandName(commandBody.Value.Span, out var commandName))
                {
                    throw new ZeroMqProtocolException("malformed command name");
                }

                if (commandName.SequenceEqual("ERROR"u8))
                {
                    throw new ZeroMqProtocolException(
                        $"peer sent ERROR: {ParseErrorReason(commandBody.Value.Span[(1 + commandName.Length)..])}");
                }

                scratchUsed = 0;
                MaybeShrinkScratch();
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
                var materialized = allocator(length, more);
                if (materialized.TryGetValue(out ZSegment single))
                {
                    if (!await TryReadExactlyAsync(single.Writable, token))
                    {
                        single.Dispose();
                        return;
                    }

                    var materializedKeepGoing = await connection.OnFrameAsync(materialized, token);
                    if (!materializedKeepGoing)
                    {
                        await WaitForResumeAsync(token);
                    }

                    continue;
                }

                if (materialized.TryGetValue(out ZSegments many))
                {
                    for (var i = 0; i < many.Count; i++)
                    {
                        var segment = many[i];
                        if (await TryReadExactlyAsync(segment.Writable, token))
                        {
                            continue;
                        }

                        many.Dispose();
                        return;
                    }

                    var multiKeepGoing = await connection.OnFrameAsync(materialized, token);
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

            // The borrowed segment refers to the scratch owner without taking
            // ownership; EnsureScratchCapacity guarantees the owner is live for
            // the duration of this frame's delivery (0006 3.4).
            if (scratchOwner is not { } source)
            {
                throw new InvalidOperationException("borrowed frame without scratch owner");
            }

            var frame = new ZFrame(ZSegment.Borrowed(source, scratchUsed, length), more);
            var keepGoing = await connection.OnFrameAsync(frame, token);
            if (!keepGoing)
            {
                await WaitForResumeAsync(token);
            }

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
        if (header.Size > maxCommandSize)
        {
            throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
        }

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
