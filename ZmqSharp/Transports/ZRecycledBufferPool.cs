using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>
/// A per-connection recycling pool of growable buffer owners (0034) with the
/// RENTED/POOLED/DEAD lifecycle: a frame owns the owner while it is outstanding,
/// disposal returns it to the pool (or releases it when the pool is full), and
/// disposing the pool releases every pooled buffer. Handle objects are allocated
/// once and reused, so the steady-state receive budget stays GC-free.
/// </summary>
internal sealed class ZRecycledBufferPool(MemoryPool<byte> pool) : IDisposable
{
    private const int Capacity = 8;
    private const int MinimumBufferSize = 4096;

    private readonly Lock _gate = new();
    private readonly Stack<ZPooledOwner> _owners = new();
    private bool _disposed;

    /// <summary>
    /// Rents an owner at least <paramref name="length"/> bytes long. A pooled
    /// owner is grown when it is too small; the pool lock is not held across
    /// the handle lock (Dispose takes them in the opposite order).
    /// </summary>
    public IMemoryOwner<byte> Rent(int length)
    {
        while (true)
        {
            ZPooledOwner? popped;
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                popped = _owners.TryPop(out var candidate) ? candidate : null;
            }

            if (popped is not { } recycled) break;
            try
            {
                if (recycled.Ensure(length)) return recycled;
            }
            catch
            {
                // A failed growth must not strand the popped owner's buffer:
                // release it before the failure escapes.
                recycled.DisposeInner();
                throw;
            }

            recycled.DisposeInner();
        }

        return new ZPooledOwner(this, pool, Math.Max(length, MinimumBufferSize));
    }

    public void Dispose()
    {
        while (true)
        {
            ZPooledOwner? owner;
            lock (_gate)
            {
                if (!_owners.TryPop(out owner))
                {
                    _disposed = true;
                    return;
                }
            }

            owner.DisposeInner();
        }
    }

    internal void Return(ZPooledOwner owner)
    {
        lock (_gate)
        {
            if (!_disposed && _owners.Count < Capacity)
            {
                _owners.Push(owner);
                return;
            }
        }

        owner.DisposeInner();
    }

    /// <summary>
    /// A pooled buffer with an explicit lifecycle: RENTED while a frame owns
    /// it, POOLED while it waits in the stack, DEAD once released. Dispose
    /// moves RENTED to POOLED exactly once, so a double dispose cannot hand one
    /// buffer to two live frames. Consumers may dispose on any thread; both
    /// locks are short leaf sections and always taken handle-then-pool.
    /// </summary>
    internal sealed class ZPooledOwner(
        ZRecycledBufferPool parent, MemoryPool<byte> pool, int length) : IMemoryOwner<byte>
    {
        private const int Rented = 0;
        private const int Pooled = 1;
        private const int Dead = 2;

        private readonly Lock _gate = new();
        private IMemoryOwner<byte>? _owner = pool.Rent(length);
        private int _state = Rented;

        public Memory<byte> Memory
        {
            get
            {
                lock (_gate)
                {
                    return _owner?.Memory ?? throw new ObjectDisposedException(nameof(ZPooledOwner));
                }
            }
        }

        /// <summary>
        /// Claims a pooled handle for a new frame (growing the buffer when a
        /// larger frame arrives); only the pool calls this. The POOLED to
        /// RENTED transition is what makes the frame's eventual Dispose
        /// recycle the buffer again.
        /// </summary>
        internal bool Ensure(int length)
        {
            lock (_gate)
            {
                if (_owner is null || _state == Dead) return false;
                if (_owner.Memory.Length < length)
                {
                    var grown = pool.Rent(Math.Max(length, _owner.Memory.Length * 2));
                    try
                    {
                        _owner.Dispose();
                    }
                    catch
                    {
                        grown.Dispose();
                        throw;
                    }

                    _owner = grown;
                }

                _state = Rented;
                return true;
            }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                if (_state != Rented || _owner is null) return;
                _state = Pooled;
            }

            parent.Return(this);
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
}
