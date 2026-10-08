using System.Buffers;

namespace ZmqSharp.Zmtp;

/// <summary>Reusable sequence nodes; the owner serializes construction and use.</summary>
internal sealed class FrameSequence
{
    private sealed class Node : ReadOnlySequenceSegment<byte>
    {
        public void Reset(ReadOnlyMemory<byte> memory, long index)
        {
            Memory = memory;
            RunningIndex = index;
            Next = null;
        }
        public void Link(Node next) => Next = next;
    }

    private readonly List<Node> nodes = [];
    private int count;
    private long length;

    public void Clear() { count = 0; length = 0; }

    public void Add(ReadOnlyMemory<byte> memory)
    {
        if (memory.IsEmpty) return;
        if (count == nodes.Count) nodes.Add(new Node());
        var node = nodes[count];
        node.Reset(memory, length);
        if (count > 0) nodes[count - 1].Link(node);
        count++;
        length += memory.Length;
    }

    public ReadOnlySequence<byte> Sequence => count switch
    {
        0 => ReadOnlySequence<byte>.Empty,
        1 => new ReadOnlySequence<byte>(nodes[0].Memory),
        _ => new ReadOnlySequence<byte>(nodes[0], 0, nodes[count - 1], nodes[count - 1].Memory.Length)
    };

    public ReadOnlySequence<byte> FromFrame(ZFrame frame)
    {
        Clear();
        for (var i = 0; i < frame.Count; i++) Add(frame[i].Memory);
        return Sequence;
    }
}
