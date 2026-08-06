using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>Linked segment node used to build ReadOnlySequence instances on demand.</summary>
internal sealed class ZSequenceSegment : ReadOnlySequenceSegment<byte>
{
    public ZSequenceSegment(ReadOnlyMemory<byte> memory, long runningIndex)
    {
        Memory = memory;
        RunningIndex = runningIndex;
    }

    public void SetNext(ZSequenceSegment next) => Next = next;
}
