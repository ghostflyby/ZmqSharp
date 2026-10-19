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

    private readonly List<Node> _nodes = [];
    private int _count;
    private long _length;

    public void Clear()
    {
        _count = 0;
        _length = 0;
    }

    public void Add(ReadOnlyMemory<byte> memory)
    {
        if (memory.IsEmpty) return;
        if (_count == _nodes.Count) _nodes.Add(new Node());
        var node = _nodes[_count];
        node.Reset(memory, _length);
        if (_count > 0) _nodes[_count - 1].Link(node);
        _count++;
        _length += memory.Length;
    }

    public ReadOnlySequence<byte> Sequence => _count switch
    {
        0 => ReadOnlySequence<byte>.Empty,
        1 => new ReadOnlySequence<byte>(_nodes[0].Memory),
        _ => new ReadOnlySequence<byte>(_nodes[0], 0, _nodes[_count - 1], _nodes[_count - 1].Memory.Length)
    };

    public ReadOnlySequence<byte> FromFrame(ZFrame frame)
    {
        Clear();
        foreach (var segment in frame)
            Add(segment.Memory);

        return Sequence;
    }
}
