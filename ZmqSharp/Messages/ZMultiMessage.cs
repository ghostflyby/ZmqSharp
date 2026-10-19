using System.Buffers;
using System.Collections;

namespace ZmqSharp;

/// <summary>Multipart message: several frames.</summary>
public readonly struct ZMultiMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZFrame[] _frames;

    internal ZMultiMessage(ZFrame[] frames)
    {
        _frames = frames;
    }

    /// <summary>
    /// Zero-copy multipart message: the caller transfers ownership of each
    /// frame array. The collection is stored, not copied; every array is
    /// disposed with the message (0026). Empty input throws - a message has
    /// at least one frame.
    /// </summary>
    public static ZMultiMessage FromOwned(byte[][] frames)
    {
        ArgumentNullException.ThrowIfNull(frames);
        if (frames.Length == 0)
            throw new ArgumentException("a message has at least one frame", nameof(frames));

        var message = new ZFrame[frames.Length];
        for (var i = 0; i < frames.Length; i++)
        {
            var frame = frames[i];
            ArgumentNullException.ThrowIfNull(frame);
            message[i] = new ZFrame(new ZSegment(frame, 0, frame.Length));
        }

        return new ZMultiMessage(message);
    }

    /// <summary>
    /// Multipart message copied frame by frame into owned buffers (0026); the
    /// caller's memories stay usable. Empty input throws - a message has at
    /// least one frame. Eagerly enumerated at construction; the enumerable is
    /// never held across an await. A throwing enumerable releases every frame
    /// rented so far before propagating.
    /// </summary>
    public static ZMultiMessage Copy(IEnumerable<ReadOnlyMemory<byte>> frames)
    {
        ArgumentNullException.ThrowIfNull(frames);
        var message = new List<ZFrame>();
        try
        {
            foreach (var frame in frames)
            {
                var owner = MemoryPool<byte>.Shared.Rent(frame.Length);
                frame.CopyTo(owner.Memory);
                message.Add(new ZFrame(new ZSegment(owner, 0, frame.Length)));
            }
        }
        catch
        {
            foreach (var frame in message) frame.Dispose();
            throw;
        }

        return message.Count == 0
            ? throw new ArgumentException("a message has at least one frame", nameof(frames))
            : new ZMultiMessage([.. message]);
    }

    /// <summary>
    /// Multipart message copied frame by frame into owned buffers (0026);
    /// accepts a <c>byte[][]</c> frame collection directly (the
    /// <c>byte[]</c> inputs are borrowed views and are copied, not retained).
    /// </summary>
    public static ZMultiMessage Copy(IEnumerable<byte[]> frames)
    {
        ArgumentNullException.ThrowIfNull(frames);
        var message = new List<ZFrame>();
        try
        {
            foreach (var frame in frames)
            {
                ArgumentNullException.ThrowIfNull(frame);
                var owner = MemoryPool<byte>.Shared.Rent(frame.Length);
                frame.CopyTo(owner.Memory);
                message.Add(new ZFrame(new ZSegment(owner, 0, frame.Length)));
            }
        }
        catch
        {
            foreach (var frame in message) frame.Dispose();
            throw;
        }

        return message.Count == 0
            ? throw new ArgumentException("a message has at least one frame", nameof(frames))
            : new ZMultiMessage([.. message]);
    }

    public int Count => _frames.Length;

    public ZFrame this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _frames.Length);
            return _frames[index];
        }
    }

    public Enumerator GetEnumerator() => new(_frames);

    IEnumerator<ZFrame> IEnumerable<ZFrame>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        foreach (var frame in _frames) frame.Dispose();
    }

    public struct Enumerator : IEnumerator<ZFrame>
    {
        private readonly ZFrame[] _frames;
        private int _index;

        internal Enumerator(ZFrame[] frames)
        {
            _frames = frames;
            _index = -1;
        }

        public ZFrame Current
        {
            get
            {
                if (_index < 0 || _index >= _frames.Length)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return _frames[_index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_index + 1 < _frames.Length)
            {
                _index++;
                return true;
            }

            _index = _frames.Length;
            return false;
        }

        public void Reset()
        {
            _index = -1;
        }

        public void Dispose() { }
    }
}
