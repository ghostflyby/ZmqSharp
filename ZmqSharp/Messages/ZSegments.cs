using System.Collections;

namespace ZmqSharp;

/// <summary>
/// NonContiguous case of a frame: a table of segments. Dispose releases every
/// pooled segment.
/// </summary>
public readonly struct ZSegments : IReadOnlyList<ZSegment>, IDisposable
{
    private readonly ZSegment[] segments;

    internal ZSegments(ZSegment[] segments)
    {
        this.segments = segments;
    }

    public int Count => segments.Length;

    public ZSegment this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, segments.Length);
            return segments[index];
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(segments);
    }

    IEnumerator<ZSegment> IEnumerable<ZSegment>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
        foreach (var segment in segments) segment.Dispose();
    }

    public struct Enumerator : IEnumerator<ZSegment>
    {
        private readonly ZSegment[] segments;
        private int index;

        internal Enumerator(ZSegment[] segments)
        {
            this.segments = segments;
            index = -1;
        }

        public ZSegment Current
        {
            get
            {
                if (index < 0 || index >= segments.Length)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return segments[index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (index + 1 < segments.Length)
            {
                index++;
                return true;
            }

            index = segments.Length;
            return false;
        }

        public void Reset()
        {
            index = -1;
        }

        public void Dispose()
        {
        }
    }
}
