using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Internal message payload: a frame table plus a segment list, shared by
/// borrowed views and owned messages. The frame table (offset/length + segment
/// index) avoids one linked heap node per frame.
/// </summary>
internal sealed class ZMessageData
{
    private readonly List<ZSegment> segments = [];
    private readonly List<FrameEntry> frames = [];

    private readonly record struct FrameEntry(int Segment, int Offset, int Length);

    public int FrameCount => frames.Count;

    public int SegmentCount => segments.Count;

    public void AddSegment(ZSegment segment) => segments.Add(segment);

    public void AddFrame(int segment, int offset, int length) => frames.Add(new(segment, offset, length));

    public ZSegment GetSegment(int index) => segments[index];

    public void SetSingleSegment(ZSegment segment)
    {
        segments.Clear();
        frames.Clear();
        segments.Add(segment);
    }

    public void Reset()
    {
        segments.Clear();
        frames.Clear();
    }

    public ZBufferOrigin GetOrigin(int frame) => segments[frames[frame].Segment].Origin;

    public ReadOnlySequence<byte> GetFrame(int index)
    {
        var entry = frames[index];
        var segment = segments[entry.Segment];
        if (entry.Offset + entry.Length <= segment.Memory.Length)
        {
            return new ReadOnlySequence<byte>(segment.Memory.Slice(entry.Offset, entry.Length));
        }

        return BuildSequence(index);
    }

    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory)
    {
        var entry = frames[index];
        var segment = segments[entry.Segment];
        if (entry.Offset + entry.Length <= segment.Memory.Length)
        {
            memory = segment.Memory.Slice(entry.Offset, entry.Length);
            return true;
        }

        memory = default;
        return false;
    }

    public ReadOnlySequence<byte> Whole
    {
        get
        {
            if (segments.Count == 1)
            {
                return new ReadOnlySequence<byte>(segments[0].Memory);
            }

            return BuildWhole();
        }
    }

    public void Dispose()
    {
        foreach (var segment in segments)
        {
            segment.Dispose();
        }

        Reset();
    }

    private ReadOnlySequence<byte> BuildSequence(int frame)
    {
        var entry = frames[frame];
        long remaining = entry.Length;
        long running = 0;
        ZSequenceSegment? head = null;
        ZSequenceSegment? tail = null;
        var segmentIndex = entry.Segment;
        var offset = entry.Offset;
        while (remaining > 0 && segmentIndex < segments.Count)
        {
            var memory = segments[segmentIndex].Memory;
            var take = (int)Math.Min(remaining, memory.Length - offset);
            var node = new ZSequenceSegment(memory.Slice(offset, take), running);
            if (head is null)
            {
                head = node;
            }
            else if (tail is { } previous)
            {
                previous.SetNext(node);
            }

            tail = node;
            running += take;
            remaining -= take;
            offset = 0;
            segmentIndex++;
        }

        if (head is { } first && tail is { } last)
        {
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }

        return ReadOnlySequence<byte>.Empty;
    }

    private ReadOnlySequence<byte> BuildWhole()
    {
        long running = 0;
        ZSequenceSegment? head = null;
        ZSequenceSegment? tail = null;
        foreach (var segment in segments)
        {
            if (segment.Memory.Length == 0)
            {
                continue;
            }

            var node = new ZSequenceSegment(segment.Memory, running);
            if (head is null)
            {
                head = node;
            }
            else
            {
                tail?.SetNext(node);
            }

            tail = node;
            running += segment.Memory.Length;
        }

        if (head is { } first && tail is { } last)
        {
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }

        return ReadOnlySequence<byte>.Empty;
    }
}
