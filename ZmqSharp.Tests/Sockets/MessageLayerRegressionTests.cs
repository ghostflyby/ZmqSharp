using Xunit;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Regression proofs for the 0033 review findings on the non-codec paths:
/// the callback tier's message disposal, the send-side retention contract
/// over a retaining transport, and pause teardown. The codec-specific
/// regressions (assembled staging, command flood, sink retention) live in
/// ZmqSharp.Security.Curve.Tests, which references the CURVE package.
/// </summary>
public sealed class MessageLayerRegressionTests
{
    [Fact(Timeout = 15_000)]
    public async Task CallbackSurface_ReleasesDeliveredMessages()
    {
        // The raw OnFrame tier once leaked one recycled owner per delivered
        // message; fifty messages must not grow the connection's rentals.
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveSurface = ZReceiveSurface.Callback,
        });
        await using var client = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(TransportKind.Tcp);
        await server.BindAsync(endpoint, token);
        await client.ConnectAsync(endpoint, token);

        var received = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        server.OnFrame += (_, _) =>
        {
            if (Interlocked.Increment(ref count) == 50) received.TrySetResult(count);
            return true;
        };

        for (var i = 0; i < 50; i++) await client.SendAsync(ZMessage.FromOwned([(byte)i]), token);
        await received.Task.WaitAsync(token);

        // The connection's lasting rentals are the framing scratch plus the
        // establishment READY's recycled owner - a handful, not fifty. A
        // per-message leak would pin fifty buffers outstanding.
        const int expectedOutstanding = 4;
        Assert.True(pool.Outstanding <= expectedOutstanding,
            $"outstanding grew to {pool.Outstanding} for 50 messages; the callback tier is leaking per message");
    }

    [Fact(Timeout = 15_000)]
    public async Task PausedOnFrame_ThenTeardown_ReleasesRetainedMessage()
    {
        // A peer paused by a false OnFrame return and then torn down must
        // release the retained message; the pause queue drains on teardown.
        using var pool = new CountingMemoryPool();
        var token = TestContext.Current.CancellationToken;
        var endpoint = TestTransports.GetEndpoint(TransportKind.Tcp);

        await using (var server = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveSurface = ZReceiveSurface.Callback,
        }))
        await using (var client = new ZPairSocket())
        {
            await server.BindAsync(endpoint, token);
            await client.ConnectAsync(endpoint, token);

            var paused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            server.OnFrame += (_, _) =>
            {
                paused.TrySetResult();
                return false;
            };

            await client.SendAsync(ZMessage.FromOwned([1]), token);
            await client.SendAsync(ZMessage.FromOwned([2]), token);
            await paused.Task.WaitAsync(token);
        }

        // Paused with the second message retained; the scope teardown above
        // released it, so no buffer is still outstanding.
        await pool.WaitForOutstandingAsync(0, TimeSpan.FromSeconds(5));
    }
}
