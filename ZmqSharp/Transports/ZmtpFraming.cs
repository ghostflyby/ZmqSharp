using System.Buffers;
using System.Buffers.Binary;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The ZMTP framing (0033): the wire dialect tcp/ipc speak, decomposed out of
/// the retired parser/encoder and now message-currency. Writes iterate a
/// message's frames - each as one logical write of flag byte, short or long
/// size, and body segments, byte-identical to every ZMTP peer. Reads parse
/// the frame grammar and re-aggregate More chains into complete messages
/// (into the receive policy's allocator segments - the zero-copy path - or
/// staged in one per-connection scratch with owned recycled copies);
/// commands surface as units for the protocol to consume. The framing is
/// codec-blind: the encoded-bound rebind and per-frame decoding belong to
/// the protocol session.
/// </summary>
public sealed class ZmtpFraming : IZFraming
{
    public const long DefaultMaxCommandSize = 1L << 20;

    public static ZmtpFraming Instance { get; } = new();

    private ZmtpFraming()
    {
    }

    /// <summary>
    /// Adapts a standalone byte connection into a message connection. The
    /// default frame bound is unbounded (limits are policy concerns - the
    /// receive policy's allocator and the command limit).
    /// </summary>
    public IZMessageConnection Adapt(IZConnection connection)
        => new ZmtpFramingConnection(connection, initialAllocator: null, long.MaxValue,
            initialPool: null, onMessageBoundary: null, ownsConnection: true);

    /// <summary>
    /// Transport composition (0034): the context supplies the establishment
    /// bound and the pool; the receive composition arrives after
    /// establishment through <see cref="IZMessageConnection.ConfigureReceive"/>.
    /// A byte connection that is its own message view passes
    /// <paramref name="ownsConnection"/> false so the view never disposes its
    /// origin.
    /// </summary>
    public IZMessageConnection Adapt(
        IZConnection connection, in ZTransportContext context, bool ownsConnection = true)
        => new ZmtpFramingConnection(connection, initialAllocator: null, context.MaxCommandSize,
            initialPool: context.Pool, onMessageBoundary: null, ownsConnection);

    /// <summary>
    /// Explicit composition: the allocator supplies the final segments per
    /// frame (the receive policy's zero-copy path); the bound caps what a
    /// single frame header may declare; the pool backs the staging scratch.
    /// </summary>
    internal IZMessageConnection Adapt(IZConnection connection, ZFrameAllocator? allocator, long bound,
        MemoryPool<byte>? pool = null, Action? onMessageBoundary = null, bool ownsConnection = true)
        => new ZmtpFramingConnection(connection, allocator, bound, pool, onMessageBoundary, ownsConnection);

    internal sealed class ZmtpFramingConnection(
        IZConnection connection, ZFrameAllocator? initialAllocator, long initialBound,
        MemoryPool<byte>? initialPool, Action? onMessageBoundary, bool ownsConnection)
        : IZMessageConnection, IZStagedFrameWriter
    {
        private readonly MemoryPool<byte> _pool = initialPool ?? MemoryPool<byte>.Shared;
        private readonly bool _ownsConnection = ownsConnection;
        private ZFrameAllocator? _allocator = initialAllocator;
        private long _bound = initialBound;
        private Action? _onMessageBoundary = onMessageBoundary;

        // One reader and one writer may overlap (0030 section 3), so the
        // read and write sides keep separate reusable state.
        private readonly byte[] _readHeader = new byte[9];
        private readonly byte[] _writeHeader = new byte[9];
        private readonly FrameSequence _writeWire = new();
        private readonly List<ZFrame> _messageFrames = [];
        private IMemoryOwner<byte>? _scratchOwner;
        private Memory<byte> _scratch;
        private readonly Lock _ownedPoolGate = new();
        private readonly Stack<RecycledOwner> _ownedPool = new();

        public async ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
        {
            try
            {
                return await ReadUnitCoreAsync(token);
            }
            catch
            {
                // A failed read mid-aggregation owns its accumulated frames;
                // release them rather than leaking half a message.
                foreach (var accumulated in _messageFrames) accumulated.Dispose();

                _messageFrames.Clear();
                throw;
            }
        }

        private async ValueTask<ZWireUnit?> ReadUnitCoreAsync(CancellationToken token)
        {
            while (true)
            {
                var read = await ReadFrameAsync(token);
                if (read is not { } wire)
                {
                    foreach (var accumulated in _messageFrames) accumulated.Dispose();

                    _messageFrames.Clear();
                    return null;
                }

                if ((wire.flags & ZmtpFrameFlags.Command) != 0)
                {
                    if (_messageFrames.Count > 0)
                        throw new ZeroMqProtocolException("command frame interrupts a More chain");

                    // A command is a single-frame control message, copied to
                    // an owned buffer so every unit honors the same caller
                    // ownership (the scratch view would die at the next
                    // read); command consumption (ERROR faulting, skipping)
                    // belongs to the protocol above - the framing is
                    // codec-blind and must not inspect possibly-encoded
                    // bodies.
                    return new ZWireUnit(ZmtpFrameFlags.Command,
                        new ZMessage(new ZSingleMessage(CopyToOwned(wire.frame))));
                }

                // Aggregated frames must be OWNED: a message outlives the
                // read loop (queues retain it), and the scratch path's views
                // die at the next read - so the staging view is copied into
                // an owned segment here (allocator frames are owned already).
                // The receive policy's allocator enforces the per-frame and
                // per-message limits itself (0008 D1).
                if (wire.frame.TryGetValue(out ZSegments _)) _messageFrames.Add(wire.frame);
                else if (wire.frame.TryGetValue(out ZSegment owned) && !owned.IsBorrowed)
                    _messageFrames.Add(wire.frame);
                else
                    _messageFrames.Add(CopyToOwned(wire.frame));

                if ((wire.flags & ZmtpFrameFlags.More) != 0) continue;

                // The boundary callback runs while the frames are still in
                // the list, so a throwing callback releases them through the
                // catch above instead of leaking a half-built message.
                _onMessageBoundary?.Invoke();
                var message = _messageFrames.Count == 1
                    ? new ZMessage(new ZSingleMessage(_messageFrames[0]))
                    : new ZMessage(new ZMultiMessage([.. _messageFrames]));
                _messageFrames.Clear();
                return new ZWireUnit(ZmtpFrameFlags.None, message);
            }
        }

        public ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
        {
            // One logical write per frame, contiguously - the message is the
            // send transaction; the wire grammar is the framing's business.
            return WriteFramesAsync(message, token);
        }

        public async ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        {
            // The pooled copy travels with the frame to the wire (a native
            // message transport carries it to the peer instead).
            var pooled = ZWireFrameLogic.Copy(ZmtpFrameFlags.Command, body.Span);
            try
            {
                await WriteFrameAsync(ZmtpFrameFlags.Command, pooled.Wire, token);
            }
            finally
            {
                pooled.Owner.Dispose();
            }
        }

        /// <summary>
        /// Applies the post-establishment receive policy in place (0034): the
        /// runtime knows the codec only after establishment, so the same
        /// connection swaps its allocator, bound, and boundary callback
        /// instead of stacking a second adapter - one scratch per connection,
        /// released exactly once.
        /// </summary>
        public void ConfigureReceive(in ZReceiveComposition composition)
        {
            _allocator = composition.Allocator;
            _bound = composition.MaxFrameLength;
            _onMessageBoundary = composition.OnMessageBoundary;
            if (composition.Allocator is not null)
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

        /// <summary>
        /// Streams one already-encoded frame directly (the codec session's
        /// fast path, 0033): one logical write, no message assembly - the
        /// public contract stays message-level, the built-in protocol and
        /// framing co-optimize their shared hot path.
        /// </summary>
        public ValueTask WriteStagedFrameAsync(ZmtpFrameFlags flags, ZFrame frame, CancellationToken token)
            => WriteFrameAsync(flags, frame, token);

        /// <summary>
        /// Copying dialect: the wire writes consume the frames during the
        /// call, so the sender keeps disposal (0033).
        /// </summary>
        public bool RetainsMessages => false;

        /// <summary>The raw byte origin this framing reads and writes.</summary>
        public IZConnection ByteOrigin => connection;

        public void Abort()
        {
            if (_ownsConnection) connection.Abort();
        }

        public void Dispose()
        {
            _scratchOwner?.Dispose();
            _scratchOwner = null;
            _scratch = Memory<byte>.Empty;
            lock (_ownedPoolGate)
            {
                while (_ownedPool.TryPop(out var recycled)) recycled.DisposeInner();
            }

            if (_ownsConnection) connection.Dispose();
        }

        private async ValueTask WriteFramesAsync(ZMessage message, CancellationToken token)
        {
            for (var i = 0; i < message.Count; i++)
            {
                var flags = i < message.Count - 1 ? ZmtpFrameFlags.More : ZmtpFrameFlags.None;
                await WriteFrameAsync(flags, message[i], token);
            }
        }

        private async ValueTask WriteFrameAsync(ZmtpFrameFlags flags, ZFrame frame, CancellationToken token)
        {
            ZWireFrameLogic.Validate(flags);
            var body = frame.ToSequence();
            var length = body.Length;
            var longSize = length > 255;
            _writeHeader[0] = (byte)(flags | (longSize ? ZmtpFrameFlags.LongSize : ZmtpFrameFlags.None));
            if (longSize) BinaryPrimitives.WriteInt64BigEndian(_writeHeader.AsSpan(1), length);
            else _writeHeader[1] = (byte)length;

            // One logical write per frame, as the wire contract demands; the
            // sequence nodes are reused across sends (single-writer).
            _writeWire.Clear();
            _writeWire.Add(_writeHeader.AsMemory(0, longSize ? 9 : 2));
            foreach (var segment in body) _writeWire.Add(segment);

            await connection.WriteAsync(_writeWire.Sequence, token);
        }

        private async ValueTask<(ZmtpFrameFlags flags, ZFrame frame)?> ReadFrameAsync(CancellationToken token)
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
            if (size > _bound)
                throw new ZeroMqProtocolException($"ZMTP frame exceeds maximum length of {_bound} bytes");
            if (size > int.MaxValue) throw new ZeroMqProtocolException("ZMTP frame exceeds supported size");

            // The long-size bit is the framing's internal marker; frames
            // carry the logical flags only.
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

                return (logical, frame);
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

                return (logical, new ZFrame(ZSegment.Borrowed(_scratch[..length]), more));
            }
        }

        /// <summary>
        /// Copies a borrowed (scratch) frame's body into a pool-owned segment;
        /// aggregated messages must survive the next read.
        /// </summary>
        private ZFrame CopyToOwned(ZFrame frame)
        {
            var sequence = frame.ToSequence();
            var length = checked((int)sequence.Length);
            var handle = RentOwned(length);
            try
            {
                sequence.CopyTo(handle.Memory[..length].Span);
                return new ZFrame(new ZSegment(handle, 0, length), frame.More);
            }
            catch
            {
                handle.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Rent from the connection's recycled owner pool: a message's
        /// dispose flows through <c>ZSegment</c> to the handle, which returns
        /// the underlying buffer here instead of the shared pool - one handle
        /// object per buffer, allocated once, reused for the connection's
        /// lifetime (the steady-state receive budget stays GC-free).
        /// </summary>
        private RecycledOwner RentOwned(int length)
        {
            while (true)
            {
                RecycledOwner? popped;
                lock (_ownedPoolGate)
                {
                    popped = _ownedPool.TryPop(out var candidate) ? candidate : null;
                }

                if (popped is not { } recycled) break;

                // Ensure runs OUTSIDE the pool lock: Dispose takes the
                // handle lock then the pool lock, so holding the pool lock
                // across Ensure would be a lock-order inversion.
                if (recycled.Ensure(length)) return recycled;

                recycled.DisposeInner();
            }

            return new RecycledOwner(this, length);
        }

        /// <summary>
        /// A recycled pool buffer with an explicit lifecycle: RENTED while a
        /// frame owns it, POOLED while it waits in the stack, DEAD once
        /// released. Dispose moves RENTED to POOLED exactly once - a second
        /// dispose of the same frame finds POOLED and does nothing, so a
        /// double dispose can never re-push one buffer to two live frames.
        /// Consumers may dispose on any thread; both locks are short leaf
        /// sections and always taken handle-then-pool.
        /// </summary>
        private sealed class RecycledOwner(ZmtpFramingConnection parent, int length) : IMemoryOwner<byte>
        {
            private const int Rented = 0;
            private const int Pooled = 1;
            private const int Dead = 2;

            private readonly Lock _gate = new();
            private IMemoryOwner<byte>? _owner = parent._pool.Rent(Math.Max(length, 4096));
            private int _state = Rented;

            public Memory<byte> Memory
            {
                get
                {
                    lock (_gate)
                    {
                        return _owner?.Memory ?? throw new ObjectDisposedException(nameof(RecycledOwner));
                    }
                }
            }

            /// <summary>
            /// Claims a pooled handle for a new frame (growing the buffer
            /// when a larger frame arrives); only RentOwned calls this. The
            /// POOLED to RENTED transition here is what makes the frame's
            /// eventual Dispose recycle the buffer again.
            /// </summary>
            public bool Ensure(int length)
            {
                lock (_gate)
                {
                    if (_owner is null || _state == Dead) return false;
                    if (_owner.Memory.Length < length)
                    {
                        var grown = parent._pool.Rent(Math.Max(length, _owner.Memory.Length * 2));
                        _owner.Dispose();
                        _owner = grown;
                    }

                    _state = Rented;
                    return true;
                }
            }

            public void Dispose()
            {
                IMemoryOwner<byte>? toRelease = null;
                lock (_gate)
                {
                    if (_state != Rented || _owner is null) return;

                    lock (parent._ownedPoolGate)
                    {
                        if (parent._ownedPool.Count < 8)
                        {
                            _state = Pooled;
                            parent._ownedPool.Push(this);
                        }
                        else
                        {
                            _state = Dead;
                            toRelease = _owner;
                            _owner = null;
                        }
                    }
                }

                toRelease?.Dispose();
            }

            internal void DisposeInner()
            {
                lock (_gate)
                {
                    _owner?.Dispose();
                    _owner = null;
                    _state = Dead;
                }
            }
        }

        private void EnsureScratchCapacity(int required)
        {
            if (_scratch.Length >= required && _scratchOwner is not null) return;

            var owner = _pool.Rent(Math.Max(required, Math.Max(4096, _scratch.Length * 2)));
            _scratchOwner?.Dispose();
            _scratchOwner = owner;
            _scratch = owner.Memory;
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

/// <summary>
/// Frame-grammar helpers the framing and protocol share (0033): flag
/// validation, pooled frame copies, and contiguous body views over the
/// library's own frame model.
/// </summary>
internal static class ZWireFrameLogic
{
    public static void Validate(ZmtpFrameFlags flags)
    {
        if ((flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            (flags & (ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid logical frame flags");
    }

    /// <summary>A frame copied into a pooled segment; dispose the owner when done.</summary>
    internal readonly struct Pooled(ZFrame frame, IDisposable owner)
    {
        public ZFrame Wire { get; } = frame;
        public IDisposable Owner { get; } = owner;
    }

    /// <summary>Copies a body into a pooled frame (cold paths: commands, greetings, codec output).</summary>
    internal static Pooled Copy(ZmtpFrameFlags flags, ReadOnlySpan<byte> body)
    {
        var owner = MemoryPool<byte>.Shared.Rent(body.Length == 0 ? 1 : body.Length);
        try
        {
            body.CopyTo(owner.Memory.Span);
            return Build(flags, owner, body.Length);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A frame's body as one contiguous memory, copying multi-segment frames
    /// (cold path); single-segment frames are returned without copy.
    /// </summary>
    internal static ReadOnlyMemory<byte> Contiguous(ZFrame frame)
    {
        var sequence = frame.ToSequence();
        if (sequence.IsSingleSegment) return sequence.First;

        var length = checked((int)sequence.Length);
        var buffer = GC.AllocateUninitializedArray<byte>(length);
        sequence.CopyTo(buffer);
        return buffer;
    }

    private static Pooled Build(ZmtpFrameFlags flags, IMemoryOwner<byte> owner, int length)
    {
        var more = (flags & ZmtpFrameFlags.More) != 0;
        return new Pooled(new ZFrame(new ZSegment(owner, 0, length), more), owner);
    }
}
