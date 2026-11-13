using System.Buffers;
using ZmqSharp.Transports;

namespace ZmqSharp.Zmtp;

/// <summary>
/// The ZWS traffic pump (0033): each WebSocket binary message is one ZMTP
/// frame - the ZMTP flag byte followed by the body, with no length prefix
/// (the message boundary is the frame boundary). Mirrors the ZMTP parser's
/// two delivery modes (materialized copy / borrowed buffer view), its
/// pause/resume gate, and its command handling; because the payload arrives
/// contiguous in the connection's receive buffer, the materialized mode
/// copies into the final segments instead of reading them directly. EOF (a
/// WebSocket close) is a connection close; protocol violations throw
/// <see cref="ZeroMqProtocolException"/>.
/// </summary>
internal sealed class ZwsPump(
    WebSocketConnection connection,
    Func<IZWirePump, ZFrame, CancellationToken, ValueTask<bool>> onFrame,
    ZFrameAllocator? allocator,
    MemoryPool<byte> pool,
    long maxCommandSize,
    IZFrameCodec? codec,
    long maxFrameLength) : IZWirePump
{
    private readonly long _maxEncodedLength =
        codec?.GetMaximumEncodedLength(Math.Max(maxFrameLength, maxCommandSize)) ?? long.MaxValue;

    private readonly Lock _gateLock = new();
    private TaskCompletionSource _gate = CreateGate();

    private static TaskCompletionSource CreateGate()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public async ValueTask ParseAsync(CancellationToken token = default)
    {
        while (true)
        {
            // The receive cap only bounds the buffer; the materializer
            // enforces the true per-frame limit at allocation, like the ZMTP
            // parser's borrowed tier.
            if (await connection.TryReceiveMessageAsync(int.MaxValue, token) is not { } message) return;

            var flags = ZwsWire.FromWireByte(message.Span[0]);
            var body = message[1..];

            if (codec is { } transform)
            {
                if (body.Length > _maxEncodedLength || body.Length > int.MaxValue)
                    throw new ZeroMqProtocolException("encoded frame exceeds maximum supported length");

                var decoded = transform.Decode(new ZmtpFrameData
                {
                    Flags = flags,
                    Body = new ReadOnlySequence<byte>(body),
                });
                ZmtpFrameEncoder.ValidateFlags(decoded.Flags);
                if ((decoded.Flags & ZmtpFrameFlags.Command) != 0)
                {
                    if (decoded.Body.Length > maxCommandSize)
                        throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
                    if (!decoded.Body.IsSingleSegment)
                        throw new ZeroMqProtocolException("decoded command must be contiguous");
                    ZmtpParser.CheckCommand(decoded.Body.First.Span);
                    continue;
                }

                var decodedLength = checked((int)decoded.Body.Length);
                var decodedMore = (decoded.Flags & ZmtpFrameFlags.More) != 0;
                ZFrame decodedFrame;
                if (allocator is not null)
                {
                    decodedFrame = allocator(decodedLength, decodedMore);
                    var remaining = decoded.Body;
                    foreach (var segment in decodedFrame)
                    {
                        remaining.Slice(0, segment.Memory.Length).CopyTo(segment.Writable.Span);
                        remaining = remaining.Slice(segment.Memory.Length);
                    }
                }
                else
                {
                    // The decoded body may be multi-segment; the pump has no
                    // scratch of its own, so a multi-segment result is copied
                    // into a pooled buffer first, like the parser's path.
                    if (!decoded.Body.IsSingleSegment)
                    {
                        var owner = pool.Rent(decodedLength);
                        try
                        {
                            decoded.Body.CopyTo(owner.Memory.Span);
                            decodedFrame = new ZFrame(ZSegment.Borrowed(owner.Memory[..decodedLength]), decodedMore);
                        }
                        catch
                        {
                            owner.Dispose();
                            throw;
                        }
                    }
                    else decodedFrame = new ZFrame(ZSegment.Borrowed(decoded.Body.First), decodedMore);
                }

                if (!await onFrame(this, decodedFrame, token)) await WaitForResumeAsync(token);
                continue;
            }

            if ((flags & ZmtpFrameFlags.Command) != 0)
            {
                if (body.Length > maxCommandSize)
                    throw new ZeroMqProtocolException($"command frame exceeds maximum size of {maxCommandSize} bytes");
                ZmtpParser.CheckCommand(body.Span);
                continue;
            }

            var length = checked((int)body.Length);
            var more = (flags & ZmtpFrameFlags.More) != 0;
            ZFrame frame;
            if (allocator is not null)
            {
                var materialized = allocator(length, more);
                try
                {
                    var remaining = new ReadOnlySequence<byte>(body);
                    foreach (var segment in materialized)
                    {
                        remaining.Slice(0, segment.Memory.Length).CopyTo(segment.Writable.Span);
                        remaining = remaining.Slice(segment.Memory.Length);
                    }
                }
                catch
                {
                    materialized.Dispose();
                    throw;
                }

                frame = materialized;
            }
            else
            {
                // Borrowed from the connection's receive buffer: valid until
                // the next receive, which happens only after delivery (and
                // any pause) completes - the same discipline as the parser's
                // scratch (0006 3.4).
                frame = new ZFrame(ZSegment.Borrowed(body), more);
            }

            var keepGoing = await onFrame(this, frame, token);
            if (!keepGoing) await WaitForResumeAsync(token);
        }
    }

    public void Resume()
    {
        lock (_gateLock)
        {
            _gate.TrySetResult();
            _gate = CreateGate();
        }
    }

    private async ValueTask WaitForResumeAsync(CancellationToken token)
    {
        Task task;
        lock (_gateLock)
        {
            task = _gate.Task;
        }

        await task.WaitAsync(token);
    }
}
