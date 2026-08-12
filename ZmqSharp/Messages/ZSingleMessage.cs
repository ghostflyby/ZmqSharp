using System.Collections;

namespace ZmqSharp;

/// <summary>Single-frame message: exactly one ZFrame.</summary>
public readonly struct ZSingleMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZFrame frame;

    internal ZSingleMessage(ZFrame frame)
    {
        this.frame = frame;
    }

    public int Count => 1;

    public ZFrame this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(index, 0);
            return frame;
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(frame);
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
        frame.Dispose();
    }

    public struct Enumerator : IEnumerator<ZFrame>
    {
        private readonly ZFrame frame;
        private int state;

        internal Enumerator(ZFrame frame)
        {
            this.frame = frame;
            state = 0;
        }

        public ZFrame Current => state != 1
            ? throw new InvalidOperationException("enumeration has not started or has already finished")
            : frame;

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (state == 0)
            {
                state = 1;
                return true;
            }

            state = 2;
            return false;
        }

        public void Reset()
        {
            state = 0;
        }

        public void Dispose()
        {
        }
    }
}
