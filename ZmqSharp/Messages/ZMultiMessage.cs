using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>Multipart message: several frames.</summary>
public readonly struct ZMultiMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZFrame[] frames;

    internal ZMultiMessage(ZFrame[] frames)
    {
        this.frames = frames;
    }

    public int Count => frames.Length;

    public ZFrame this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, frames.Length);
            return frames[index];
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(frames);
    }

    IEnumerator<ZFrame> IEnumerable<ZFrame>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
        foreach (var frame in frames) frame.Dispose();
    }

    public struct Enumerator : IEnumerator<ZFrame>
    {
        private readonly ZFrame[] frames;
        private int index;

        internal Enumerator(ZFrame[] frames)
        {
            this.frames = frames;
            index = -1;
        }

        public ZFrame Current
        {
            get
            {
                if (index < 0 || index >= frames.Length)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return frames[index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (index + 1 < frames.Length)
            {
                index++;
                return true;
            }

            index = frames.Length;
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
