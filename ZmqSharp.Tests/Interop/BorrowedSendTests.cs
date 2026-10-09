using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
/// The borrowed send semantics of <c>SendAsync(ReadOnlyMemory&lt;byte&gt;)</c>
/// and the REQ/REP/ROUTER variants (0026 3.6): the caller's buffer of any
/// backing (<c>byte[]</c>, <c>string</c>, a custom
/// <see cref="MemoryManager{T}"/>) is sent without copying - zero pool rent -
/// and is free again once the awaited send completes. The caller must not
/// modify it until then.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class BorrowedSendTests
{
    [Fact(Timeout = 15_000)]
    public async Task Pair_SendAsync_BorrowsByteArray_ZeroRent_DeliversExactBytes()
    {
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        var payload = "borrowed"u8.ToArray();
        await client.SendAsync(payload, token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal(payload);
        message.Value.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task Pair_SendAsync_BorrowsCustomMemoryManagerBacking()
    {
        // A custom MemoryManager backing is neither byte[] nor string; the
        // borrow still sends it without copying - the borrowed view holds any
        // ReadOnlyMemory<byte> backing.
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        using var manager = new FixedMemoryManager("manager-backed"u8.ToArray());
        await client.SendAsync(manager.Memory, token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "manager-backed"u8]);
        message.Value.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task BorrowedSend_AfterAwait_CallerBufferIsFreeAgain()
    {
        // The BCL write contract: the buffer may be reused once the awaited
        // write completes. Mutating it after the await must not affect the
        // already-sent frame.
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        var payload = "before"u8.ToArray();
        await client.SendAsync(payload, token);
        payload[0] = (byte)'X';

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "before"u8]);
        message.Value.Dispose();
    }

    [Fact(Timeout = 15_000)]
    public async Task Dealer_SendAsync_BorrowsAndRoundTripsThroughNetMQRouter()
    {
        using var router = new RouterSocket();
        router.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        router.Bind($"tcp://127.0.0.1:{port}");

        await using var dealer = new ZDealerSocket();
        var token = TestContext.Current.CancellationToken;
        await dealer.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        await dealer.SendAsync("hello"u8.ToArray(), token);

        var message = new NetMQMessage();
        router.TryReceiveMultipartMessage(TimeSpan.FromSeconds(5), ref message).Should().BeTrue();
        message.Should().NotBeNull();
        message.FrameCount.Should().Be(2); // routing id + payload
        message[1].ToByteArray().Should().Equal([.. "hello"u8]);
    }

    [Fact(Timeout = 15_000)]
    public async Task BorrowedSend_RentsNothingFromThePool()
    {
        // The client's pool is the send-side pool; a borrowed send must not
        // rent from it (the server's pool rents on the receive path, so the
        // counter is on the client only).
        using var pool = new CountingRentPool();
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket(new ZSocketOptions { Pool = pool });
        var port = InteropHelpers.GetFreePort();
        var token = TestContext.Current.CancellationToken;
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        await client.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        // The handshake rents the mechanism scratch from the pool; record that
        // baseline and assert the borrowed send adds nothing.
        var baseline = pool.Rentals;
        await client.SendAsync("borrowed"u8.ToArray(), token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        message.Should().NotBeNull();
        message.Value.Dispose();

        // Zero-copy borrow: nothing rented from the send-side pool.
        pool.Rentals.Should().Be(baseline);
    }

    private static async Task<ZMessage?> ReadMessageAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan timeout,
        CancellationToken token)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(timeout);
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return null;
        }
    }

    private sealed class CountingRentPool : MemoryPool<byte>
    {
        private readonly MemoryPool<byte> inner = Shared;
        private int rented;

        public int Rentals => Volatile.Read(ref rented);

        public override int MaxBufferSize => inner.MaxBufferSize;

        public override IMemoryOwner<byte> Rent(int minimumBufferSize = -1)
        {
            Interlocked.Increment(ref rented);
            return inner.Rent(minimumBufferSize);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
        }
    }

    private sealed class FixedMemoryManager(byte[] backing) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => backing;

        public override MemoryHandle Pin(int elementIndex = 0) => new();

        public override void Unpin() { }

        protected override void Dispose(bool disposing) { }
    }
}
