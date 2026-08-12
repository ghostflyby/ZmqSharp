using System.Collections;

namespace ZmqSharp;

/// <summary>
/// A message with two cases: Single (one frame) or Multi (several frames).
/// The cases are types (ZSingleMessage / ZMultiMessage); this type is constructed from one
/// of them and exposes each case through a TryGetValue overload.
/// </summary>
public readonly struct ZMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZSingleMessage? single; // Single case
    private readonly ZMultiMessage? multi; // Multi case

    public ZMessage(ZSingleMessage single)
    {
        this.single = single;
    }

    public ZMessage(ZMultiMessage multi)
    {
        this.multi = multi;
    }

    /// <summary>Implicit conversion from the single case (0005).</summary>
    public static implicit operator ZMessage(ZSingleMessage single)
    {
        return new ZMessage(single);
    }

    /// <summary>Implicit conversion from the multi case (0005).</summary>
    public static implicit operator ZMessage(ZMultiMessage multi)
    {
        return new ZMessage(multi);
    }

    /// <summary>Builds a single-frame owned message (zero copy; Dispose never touches a pool).</summary>
    public static ZMessage FromOwned(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        return new ZMessage(new ZSingleMessage(new ZFrame(new ZSegment(data, 0, data.Length))));
    }

    public bool TryGetValue(out ZSingleMessage singleMessage)
    {
        singleMessage = single.GetValueOrDefault();
        return single is not null;
    }

    public bool TryGetValue(out ZMultiMessage multiMessage)
    {
        multiMessage = multi.GetValueOrDefault();
        return multi is not null;
    }

    public int Count => multi?.Count ?? 1;

    public ZFrame this[int index]
        => multi is null ? single.GetValueOrDefault()[index] : multi.Value[index];

    public Enumerator GetEnumerator()
    {
        return multi is null ? new Enumerator(single.GetValueOrDefault()) : new Enumerator(multi.Value);
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
        single?.Dispose();
        multi?.Dispose();
    }

    public struct Enumerator : IEnumerator<ZFrame>
    {
        private readonly ZSingleMessage? singleMessage;
        private readonly ZMultiMessage? multiMessage;
        private int index;

        internal Enumerator(ZSingleMessage singleMessage)
        {
            this.singleMessage = singleMessage;
            multiMessage = null;
            index = -1;
        }

        internal Enumerator(ZMultiMessage multiMessage)
        {
            this.multiMessage = multiMessage;
            singleMessage = null;
            index = -1;
        }

        public ZFrame Current
        {
            get
            {
                var count = singleMessage is not null ? 1 : multiMessage.GetValueOrDefault().Count;
                if (index < 0 || index >= count)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return singleMessage is not null
                    ? singleMessage.GetValueOrDefault()[index]
                    : multiMessage.GetValueOrDefault()[index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            var count = singleMessage is not null ? 1 : multiMessage.GetValueOrDefault().Count;
            if (index + 1 < count)
            {
                index++;
                return true;
            }

            index = count;
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
