using System.Buffers;
using System.Collections;

namespace ZmqSharp.Messages;

/// <summary>
/// A frame with two cases: Contiguous (one segment) or NonContiguous (a
/// segment table). Constructed from the case type; every case getter is a
/// TryGetValue overload.
/// </summary>
public readonly struct ZFrame : IReadOnlyList<ZSegment>, IDisposable
{
    private readonly ZSegment? contiguous; // Contiguous case
    private readonly ZSegments? nonContiguous; // NonContiguous case

    public ZFrame(ZSegment segment)
    {
        contiguous = segment;
    }

    public ZFrame(ZSegments segments)
    {
        nonContiguous = segments;
    }

    /// <summary>Implicit conversion from the contiguous case (0005).</summary>
    public static implicit operator ZFrame(ZSegment segment)
    {
        return new ZFrame(segment);
    }

    /// <summary>Implicit conversion from the non-contiguous case (0005).</summary>
    public static implicit operator ZFrame(ZSegments segments)
    {
        return new ZFrame(segments);
    }

    internal ZFrame(ZSegment segment, bool more)
    {
        contiguous = segment;
        More = more;
    }

    internal ZFrame(ZSegments segments, bool more)
    {
        nonContiguous = segments;
        More = more;
    }

    /// <summary>True when more frames of the same message follow.</summary>
    public bool More { get; }

    public bool TryGetValue(out ZSegment segment)
    {
        segment = contiguous.GetValueOrDefault();
        return contiguous is not null;
    }

    public bool TryGetValue(out ZSegments segments)
    {
        segments = nonContiguous.GetValueOrDefault();
        return nonContiguous is not null;
    }

    public int Count => nonContiguous?.Count ?? 1;

    public ZSegment this[int index]
        => nonContiguous is null ? SingleSegment(index) : nonContiguous.Value[index];

    public Enumerator GetEnumerator()
    {
        return new Enumerator(contiguous, nonContiguous);
    }

    IEnumerator<ZSegment> IEnumerable<ZSegment>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>Materializes the frame content as a sequence.</summary>
    public ReadOnlySequence<byte> ToSequence()
    {
        if (contiguous is { } single) return new ReadOnlySequence<byte>(single.Memory);

        if (nonContiguous is { } many) return ZSequence.Build(IterateSegments(many));

        return ReadOnlySequence<byte>.Empty;
    }

    public void Dispose()
    {
        contiguous?.Dispose();
        nonContiguous?.Dispose();
    }

    private ZSegment SingleSegment(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(index, 0);
        return contiguous.GetValueOrDefault();
    }

    private static IEnumerable<ReadOnlyMemory<byte>> IterateSegments(ZSegments segments)
    {
        foreach (var t in segments) yield return t.Memory;
    }

    public struct Enumerator : IEnumerator<ZSegment>
    {
        private readonly ZSegment? single;
        private readonly ZSegments? many;
        private int index;

        internal Enumerator(ZSegment? single, ZSegments? many)
        {
            this.single = single;
            this.many = many;
            index = -1;
        }

        public ZSegment Current
        {
            get
            {
                var count = single is not null ? 1 : many.GetValueOrDefault().Count;
                if (index < 0 || index >= count)
                    throw new InvalidOperationException("enumeration has not started or has already finished");

                return single ?? many.GetValueOrDefault()[index];
            }
        }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            var count = single is not null ? 1 : many.GetValueOrDefault().Count;
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

/// <summary>
/// Transient ReadOnlySequence building over the BCL ReadOnlySequenceSegment
/// facility. Nodes are created per call and never stored.
/// </summary>
internal static class ZSequence
{
    internal static ReadOnlySequence<byte> Build(IEnumerable<ReadOnlyMemory<byte>> segments)
    {
        using var enumerator = segments.GetEnumerator();
        if (!enumerator.MoveNext()) return ReadOnlySequence<byte>.Empty;

        var first = enumerator.Current;
        if (!enumerator.MoveNext()) return new ReadOnlySequence<byte>(first);

        ZSequenceSegment head = new(first, 0);
        var tail = head;
        var running = first.Length;
        do
        {
            var memory = enumerator.Current;
            var node = new ZSequenceSegment(memory, running);
            tail.SetNext(node);
            tail = node;
            running += memory.Length;
        } while (enumerator.MoveNext());

        return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
    }
}

internal sealed class ZSequenceSegment : ReadOnlySequenceSegment<byte>
{
    internal ZSequenceSegment(ReadOnlyMemory<byte> memory, long runningIndex)
    {
        Memory = memory;
        RunningIndex = runningIndex;
    }

    internal void SetNext(ZSequenceSegment next)
    {
        Next = next;
    }
}
