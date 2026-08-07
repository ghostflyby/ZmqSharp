using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>Internal message construction helpers for the socket bridge.</summary>
internal static class ZMessageFactory
{
    public static ZMessage Materialize(ZMessageView view, MemoryPool<byte> pool)
    {
        var data = new ZMessageData();
        for (int i = 0; i < view.FrameCount; i++)
        {
            var frame = view.GetFrame(i);
            int length = checked((int)frame.Length);
            var owner = pool.Rent(length);
            CopyTo(frame, owner.Memory);
            data.AddSegment(new ZSegment
            {
                Origin = ZBufferOrigin.Pooled,
                Owner = owner,
                Memory = owner.Memory[..length],
            });
            data.AddFrame(data.SegmentCount - 1, 0, length);
        }

        return new ZMessage(data);
    }

    public static ZMessage CopyToPooled(ReadOnlyMemory<byte> bytes, MemoryPool<byte> pool)
    {
        var owner = pool.Rent(bytes.Length);
        bytes.CopyTo(owner.Memory);
        var data = new ZMessageData();
        data.AddSegment(new ZSegment
        {
            Origin = ZBufferOrigin.Pooled,
            Owner = owner,
            Memory = owner.Memory[..bytes.Length],
        });
        data.AddFrame(0, 0, bytes.Length);
        return new ZMessage(data);
    }

    private static void CopyTo(ReadOnlySequence<byte> source, Memory<byte> target)
    {
        int offset = 0;
        foreach (var memory in source)
        {
            memory.Span.CopyTo(target.Span[offset..]);
            offset += memory.Length;
        }
    }
}
