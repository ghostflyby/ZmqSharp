using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The ZMTP framing (0033): one frame per flag-and-length-prefixed unit on a
/// byte connection - the wire dialect tcp/ipc speak today, decomposed out of
/// the former public parser/encoder. The reader materializes whole owned
/// frames (into the caller's allocator segments when the runtime supplies
/// one - preserving the byte path's zero-copy - or into pooled segments);
/// the writer gathers the flag byte, the short or long size, and the body
/// segments into one logical write, exactly as the encoder always did.
/// </summary>
public sealed class ZmtpFraming : IZFraming
{
    /// <summary>Default command-size limit (0008 Slice B).</summary>
    public const long DefaultMaxCommandSize = 1L << 20;

    public static ZmtpFraming Instance { get; } = new();

    private ZmtpFraming()
    {
    }

    /// <summary>
    /// Adapts a byte connection into a frame connection. The default bound is
    /// unbounded (limits are policy concerns - the receive policy's allocator
    /// and the command channel's command limit); supply the internal overload
    /// to cap what the reader will materialize.
    /// </summary>
    public IZFrameConnection Adapt(IZConnection connection)
        => new ZmtpFramingConnection(connection, initialAllocator: null, long.MaxValue, initialPool: null);

    /// <summary>
    /// Runtime composition: the allocator supplies the final segments for
    /// materialized frames (the receive policy's zero-copy path); the bound
    /// caps the frame length the reader will accept; the pool backs the
    /// pooled path (no allocator) - the socket pool, so allocation budgets
    /// observe its rents.
    /// </summary>
    internal IZFrameConnection Adapt(IZConnection connection, ZFrameAllocator? allocator, long maxFrameLength,
        MemoryPool<byte>? pool = null)
        => new ZmtpFramingConnection(connection, allocator, maxFrameLength, pool);

    internal sealed class ZmtpFramingConnection(
        IZConnection connection, ZFrameAllocator? initialAllocator, long initialMaxFrameLength,
        MemoryPool<byte>? initialPool) : IZFrameConnection
    {
        private ZFrameAllocator? _allocator = initialAllocator;
        private long _maxFrameLength = initialMaxFrameLength;
        private readonly MemoryPool<byte> _pool = initialPool ?? MemoryPool<byte>.Shared;

        // One reader and one writer may overlap (0030 section 3), so the
        // read and write sides keep separate reusable state.
        private readonly byte[] _readHeader = new byte[9];
        private readonly byte[] _writeHeader = new byte[9];
        private readonly FrameSequence _writeWire = new();
        private IMemoryOwner<byte>? _scratchOwner;
        private Memory<byte> _scratch;

        private void EnsureScratchCapacity(int required)
        {
            if (_scratch.Length >= required && _scratchOwner is not null) return;

            var owner = _pool.Rent(Math.Max(required, Math.Max(4096, _scratch.Length * 2)));
            _scratchOwner?.Dispose();
            _scratchOwner = owner;
            _scratch = owner.Memory;
        }

        public async ValueTask<ZWireFrame?> ReadFrameAsync(CancellationToken token = default)
        {
            // Flag byte + size: exactly the header the parser always read.
            if (!await TryReadExactlyAsync(_readHeader.AsMemory(0, 1), token)) return null;

            var flags = (ZmtpFrameFlags)_readHeader[0];
            if ((flags & (ZmtpFrameFlags)0b1111_1000) != 0)
                throw new ZeroMqProtocolException("reserved ZMTP frame flag bits are set");

            if ((flags & ZmtpFrameFlags.Command) != 0 && (flags & ZmtpFrameFlags.More) != 0)
                throw new ZeroMqProtocolException("command frame cannot carry the MORE flag");

            var isLong = (flags & ZmtpFrameFlags.LongSize) != 0;
            var sizeLength = isLong ? 8 : 1;
            if (!await TryReadExactlyAsync(_readHeader.AsMemory(1, sizeLength), token)) return null;

            var size = isLong
                ? BinaryPrimitives.ReadInt64BigEndian(_readHeader.AsSpan(1, 8))
                : _readHeader[1];
            if (size < 0) throw new ZeroMqProtocolException("negative ZMTP frame size");
            if (size > _maxFrameLength)
                throw new ZeroMqProtocolException($"ZMTP frame exceeds maximum length of {_maxFrameLength} bytes");
            if (size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

            // The long-size bit is the framing's internal marker; the frame
            // layer carries the logical flags only.
            var logical = flags & ~ZmtpFrameFlags.LongSize;
            var more = (logical & ZmtpFrameFlags.More) != 0;

            var length = (int)size;
            if (_allocator is { } materialize)
            {
                var frame = materialize(length, more);
                try
                {
                    foreach (var segment in frame)
                        if (!await TryReadExactlyAsync(segment.Writable, token))
                        {
                            frame.Dispose();
                            return null;
                        }
                }
                catch
                {
                    frame.Dispose();
                    throw;
                }

                return new ZWireFrame(logical, frame);
            }
            else
            {
                // The pooled path stages into one reused per-connection buffer
                // (grown on demand) and hands out borrowed views - the old
                // parser's scratch discipline (0006 3.4): a frame is valid
                // until the next read, a dispose is a no-op, and the steady
                // state rents nothing.
                EnsureScratchCapacity(length);
                if (!await TryReadExactlyAsync(_scratch[..length], token)) return null;

                return new ZWireFrame(logical, new ZFrame(ZSegment.Borrowed(_scratch[..length]), more));
            }
        }

        public async ValueTask WriteFrameAsync(ZWireFrame frame, CancellationToken token = default)
        {
            ZWireFrame.Validate(frame.Flags);
            var body = frame.Frame.ToSequence();
            var length = body.Length;
            var longSize = length > 255;
            _writeHeader[0] = (byte)(frame.Flags | (longSize ? ZmtpFrameFlags.LongSize : ZmtpFrameFlags.None));
            if (longSize) BinaryPrimitives.WriteInt64BigEndian(_writeHeader.AsSpan(1), length);
            else _writeHeader[1] = (byte)length;

            // One logical write per frame, as the wire contract demands; the
            // sequence nodes are reused across sends (single-writer).
            _writeWire.Clear();
            _writeWire.Add(_writeHeader.AsMemory(0, longSize ? 9 : 2));
            foreach (var segment in body) _writeWire.Add(segment);

            await connection.WriteAsync(_writeWire.Sequence, token);
        }

        /// <summary>
        /// Rebinds the reader's policy in place (0033): the runtime knows the
        /// codec only after establishment, so the same connection swaps its
        /// allocator, bound and pool instead of stacking a second adapter -
        /// one scratch per connection, released exactly once.
        /// </summary>
        internal void Rebind(ZFrameAllocator? reboundAllocator, long reboundMaxFrameLength)
        {
            _allocator = reboundAllocator;
            _maxFrameLength = reboundMaxFrameLength;
            if (reboundAllocator is not null)
            {
                // A materializing allocator reads frames into its own
                // segments; the establishment-phase staging scratch is dead
                // weight now, so release it instead of holding it for the
                // connection's lifetime.
                _scratchOwner?.Dispose();
                _scratchOwner = null;
                _scratch = Memory<byte>.Empty;
            }
        }

        public void Abort() => connection.Abort();

        public void Dispose()
        {
            _scratchOwner?.Dispose();
            _scratchOwner = null;
            _scratch = Memory<byte>.Empty;
            connection.Dispose();
        }

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
    }
}
