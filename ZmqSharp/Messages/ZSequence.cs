using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Transient ReadOnlySequence building over the BCL ReadOnlySequenceSegment
/// facility. Nodes are created per call and never stored by messages.
/// </summary>
internal static class ZSequence
{
    public static ReadOnlySequence<byte> Build(ReadOnlyMemory<byte>[] segments)
    {
        if (segments.Length == 1)
        {
            return new ReadOnlySequence<byte>(segments[0]);
        }

        if (segments.Length == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        ZSequenceSegment? head = null;
        ZSequenceSegment? tail = null;
        var running = 0L;
        foreach (var memory in segments)
        {
            var node = new ZSequenceSegment(memory, running);
            if (head is null)
            {
                head = node;
            }
            else if (tail is { } previous)
            {
                previous.SetNext(node);
            }

            tail = node;
            running += memory.Length;
        }

        if (head is { } first && tail is { } last)
        {
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }

        return ReadOnlySequence<byte>.Empty;
    }
}

internal sealed class ZSequenceSegment : ReadOnlySequenceSegment<byte>
{
    public ZSequenceSegment(ReadOnlyMemory<byte> memory, long runningIndex)
    {
        Memory = memory;
        RunningIndex = runningIndex;
    }

    public void SetNext(ZSequenceSegment next) => Next = next;
}
