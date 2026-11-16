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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Dispose_ReleasesRemainingAndLateOwners_AndClosesThePool(bool ownerThrows)
    {
        using var pool = new DisposalTrackingPool(ownerThrows ? [2] : []);
        var buffers = new ZRecycledBufferPool(pool);
        var first = buffers.Rent(1);
        var second = buffers.Rent(1);
        var late = buffers.Rent(1);
        first.Dispose();
        second.Dispose();

        if (ownerThrows)
        {
            var failure = Assert.Throws<IOException>(buffers.Dispose);
            Assert.Equal("owner 2 disposal failed", failure.Message);
        }
        else
            buffers.Dispose();

        Assert.Equal([2, 1], pool.DisposalAttempts);
        Assert.Throws<ObjectDisposedException>(() => buffers.Rent(1));
        Assert.Throws<ObjectDisposedException>(() => first.Memory);
        Assert.Throws<ObjectDisposedException>(() => second.Memory);

        late.Dispose();
        Assert.Equal([2, 1, 3], pool.DisposalAttempts);

        buffers.Dispose();
        first.Dispose();
        second.Dispose();
        late.Dispose();
        Assert.Equal([2, 1, 3], pool.DisposalAttempts);
    }

    [Fact]
    public void Dispose_ReportsAllFailures_AfterReleasingEveryPooledOwner()
    {
        using var pool = new DisposalTrackingPool([1, 3]);
        var buffers = new ZRecycledBufferPool(pool);
        var first = buffers.Rent(1);
        var second = buffers.Rent(1);
        var third = buffers.Rent(1);
        first.Dispose();
        second.Dispose();
        third.Dispose();

        var failure = Assert.Throws<AggregateException>(buffers.Dispose);

        Assert.Equal([3, 2, 1], pool.DisposalAttempts);
        Assert.All(failure.InnerExceptions, exception => Assert.IsType<IOException>(exception));
        Assert.Equal(["owner 3 disposal failed", "owner 1 disposal failed"],
            failure.InnerExceptions.Select(exception => exception.Message));
        Assert.Throws<ObjectDisposedException>(() => buffers.Rent(1));
        buffers.Dispose();
        Assert.Equal([3, 2, 1], pool.DisposalAttempts);
    }

    private sealed class DisposalTrackingPool(int[] failingOwners) : MemoryPool<byte>
    {
        private readonly HashSet<int> _failingOwners = [.. failingOwners];
        private int _rents;

        public List<int> DisposalAttempts { get; } = [];

        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
            => new TrackingOwner(this, new byte[Math.Max(minimumBufferSize, 1)], ++_rents);

        protected override void Dispose(bool disposing)
        {
        }

        private sealed class TrackingOwner(DisposalTrackingPool pool, byte[] buffer, int id)
            : IMemoryOwner<byte>
        {
            public Memory<byte> Memory => buffer;

            public void Dispose()
            {
                pool.DisposalAttempts.Add(id);
                if (pool._failingOwners.Contains(id)) throw new IOException($"owner {id} disposal failed");
            }
        }
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
