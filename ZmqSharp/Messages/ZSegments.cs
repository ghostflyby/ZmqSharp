using System.Collections;

namespace ZmqSharp;

/// <summary>
/// NonContiguous case of a frame: a table of segments. Dispose releases every
/// pooled segment.
/// </summary>
public readonly struct ZSegments : IReadOnlyList<ZSegment>, IDisposable
{
    private readonly ZSegment[] _segments;

    internal ZSegments(ZSegment[] segments)
    {
        _segments = segments;
    }

    public int Count => _segments.Length;

    public ZSegment this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _segments.Length);
            return _segments[index];
        }
    }

    public Enumerator GetEnumerator() => new(_segments);

    IEnumerator<ZSegment> IEnumerable<ZSegment>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        foreach (var segment in _segments) segment.Dispose();
    }

    public struct Enumerator : IEnumerator<ZSegment>
    {
        private readonly ZSegment[] _segments;
        private int _index;

        internal Enumerator(ZSegment[] segments)
        {
            _segments = segments;
            _index = -1;
        }

        public ZSegment Current
        {
            get
            {
                if (_index < 0 || _index >= _segments.Length)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return _segments[_index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_index + 1 < _segments.Length)
            {
                _index++;
                return true;
            }

            _index = _segments.Length;
            return false;
        }

        public void Reset()
        {
            _index = -1;
        }

        public void Dispose() { }
    }
}
