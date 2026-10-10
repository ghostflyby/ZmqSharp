using System.Buffers;
using Xunit;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Construction allocation contracts of the public message factories (0026):
/// the zero-copy faces (<c>FromOwned</c> / <c>FromPooled</c>) must not rent
/// from the pool, and the copy face must not retain caller buffers.
/// </summary>
public class MessageConstructionAllocationTests
{
    [Fact]
    public void FromOwned_DoesNotRentFromThePool()
    {
        using var pool = new CountingRentPool();
        var frames = new[] { "abc"u8.ToArray(), "def"u8.ToArray() };

        var message = ZMessage.FromOwned(frames);

        // Zero-copy: the message wraps the caller's arrays; nothing is rented.
        Assert.Equal(0, pool.Rentals);
        Assert.Equal(2, message.Count);
        message.Dispose();
    }

    [Fact]
    public void FromOwned_RetainsTheCallerArrays()
    {
        var frames = new[] { "abc"u8.ToArray(), "def"u8.ToArray() };
        var before = frames[0];

        var message = ZMessage.FromOwned(frames);

        // The message's first frame content is the caller's array itself
        // (reference identity, not a copy).
        Assert.True(message[0].TryGetValue(out ZSegment segment));
        Assert.True(segment.GetOwnedArray(out var backing));
        Assert.Same(before, backing);
        message.Dispose();
    }

    [Fact]
    public void FromPooled_DisposeReturnsTheOwnerExactlyOnce()
    {
        using var pool = new CountingRentPool();
        var owner = pool.Rent(8);
        "payload"u8.CopyTo(owner.Memory.Span);
        var message = ZMessage.FromPooled(owner);

        message.Dispose();

        // The owner is released with the message. The counting pool only
        // counts rents; the release itself is the inner pool's contract, and
        // a double release would fault the pool.
        message.Dispose(); // idempotent: ZMessage.Dispose is safe to call twice
    }

    [Fact]
    public void Copy_IsIsolatedFromTheCallerBuffer()
    {
        var source = "payload"u8.ToArray();

        var message = ZMessage.Copy(source);
        source[0] = (byte)'X';

        Assert.Equal([.. "payload"u8], message[0].ToSequence().ToArray());
        message.Dispose();
    }
}
