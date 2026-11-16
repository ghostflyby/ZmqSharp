using System.Buffers;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Transports;

public sealed class RecycledBufferPoolTests
{
    [Fact]
    public void GrowthFailure_ReleasesThePoppedBuffer()
    {
        var pool = new FaultingPool(failAtRent: 2);
        var buffers = new ZRecycledBufferPool(pool);

        var owner = buffers.Rent(1);
        owner.Dispose(); // returns the initial 4096-byte owner to the recycle stack

        Assert.Throws<IOException>(() => buffers.Rent(8192)); // the growth rent fails
        Assert.True(pool.FirstOwnerDisposed); // the popped buffer was released, not lost
    }

    private sealed class FaultingPool(int failAtRent) : MemoryPool<byte>
    {
        private int _rents;

        public bool FirstOwnerDisposed { get; private set; }

        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
        {
            var count = Interlocked.Increment(ref _rents);
            if (count == failAtRent) throw new IOException("growth rent failed");

            return new TrackingOwner(this, new byte[Math.Max(minimumBufferSize, 1)], count == 1);
        }

        protected override void Dispose(bool disposing)
        {
        }

        private sealed class TrackingOwner(FaultingPool pool, byte[] buffer, bool first)
            : IMemoryOwner<byte>
        {
            private int _disposed;

            public Memory<byte> Memory => buffer;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
                if (first) pool.FirstOwnerDisposed = true;
            }
        }
    }
}
