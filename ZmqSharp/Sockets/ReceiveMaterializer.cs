using System.Buffers;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

internal sealed class ReceiveMaterializer(
    MemoryPool<byte> pool,
    IZReceivePolicy policy,
    long maxFrameLength,
    long maxMessageLength,
    int maxFramesPerMessage,
    Action onRejected)
{
    private const int SegmentBlockSize = 8192;

    private int frameIndex;
    private long accumulatedLength;

    public ZFrameAllocator CreateAllocator()
    {
        return (length, more) =>
        {
            var frameIndex = this.frameIndex;
            if (!ZReceiveGuard.TryAccumulate(accumulatedLength, length, out var accumulated))
                // An unrepresentable message total is a rejection, never an
                // arithmetic exception (0008 D3/D6).
                Reject(ZReceiveRejectionReason.MessageTooLarge, null, null);

            this.frameIndex = frameIndex + 1;
            accumulatedLength = accumulated;

            // Connection-level limits are enforced here, before any allocation,
            // so a custom policy can never bypass them (0008 D1).
            if (ZReceiveGuard.CheckLimits(
                    length,
                    accumulated,
                    frameIndex,
                    maxFrameLength,
                    maxMessageLength,
                    maxFramesPerMessage) is { } rejection)
                Reject(rejection);

            var allocation = policy.Decide(new ZReceiveContext
            {
                FrameLength = length,
                HasMore = more,
                FrameIndex = frameIndex,
                AccumulatedLength = accumulated
            });
            return AllocateSegments(allocation, length, more);
        };
    }

    /// <summary>Resets the guard counters at a message boundary.</summary>
    public void Reset()
    {
        frameIndex = 0;
        accumulatedLength = 0;
    }

    private void Reject(ZReceiveRejection rejection)
    {
        onRejected();
        throw new ZReceiveRejectedException(rejection);
    }

    private void Reject(ZReceiveRejectionReason reason, long? limit, long? actual)
    {
        Reject(new ZReceiveRejection { Reason = reason, Limit = limit, Actual = actual });
    }

    private (object Owner, int Length) Allocate(ZReceiveMode mode, int length)
    {
        if (mode == ZReceiveMode.Owned)
        {
            var buffer = GC.AllocateUninitializedArray<byte>(length);
            return (buffer, length);
        }

        var owner = pool.Rent(length);
        return (owner, length);
    }

    private ZFrame AllocateSegments(
        ZReceiveAllocation allocation,
        int length,
        bool more)
    {
        if (allocation.Segmented && length > SegmentBlockSize)
        {
            var count = (int)(((long)length + SegmentBlockSize - 1) / SegmentBlockSize);
            var segments = new ZSegment[count];
            var offset = 0;
            var allocated = 0;
            try
            {
                for (; allocated < count; allocated++)
                {
                    var blockLength = Math.Min(SegmentBlockSize, length - offset);
                    var (owner, _) = Allocate(allocation.Mode, blockLength);
                    segments[allocated] = new ZSegment(owner, 0, blockLength);
                    offset += blockLength;
                }
            }
            catch
            {
                for (var i = 0; i < allocated; i++) segments[i].Dispose();
                throw;
            }

            return new ZFrame(new ZSegments(segments), more);
        }

        var (singleOwner, _) = Allocate(allocation.Mode, length);
        return new ZFrame(new ZSegment(singleOwner, 0, length), more);
    }
}
