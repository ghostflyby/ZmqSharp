using System.Buffers;
using System.Text;
using System.Threading.Channels;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Tests.Interop;

/// <summary>
///     PUSH/PULL interop with the NetMQ libzmq-compatible implementation over TCP
///     (0006 section 5): send-only round-robin outbound and receive-only
///     fair-queue inbound, in both directions.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class PushPullInteropTests
{
    [Fact(Timeout = 15_000)]
    public async Task ZmqSharpPush_NetMQPull_Delivers()
    {
        using var pull = new PullSocket();
        pull.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        pull.Bind($"tcp://127.0.0.1:{port}");

        await using var push = new ZPushSocket();
        var token = TestContext.Current.CancellationToken;
        await push.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        for (var i = 0; i < 5; i++)
        {
            await push.SendAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"msg-{i}")), token);
            var received = InteropHelpers.ReceiveFrame(pull, TimeSpan.FromSeconds(5));
            Assert.Equal(Encoding.ASCII.GetBytes($"msg-{i}"), received);
        }
    }

    [Fact]
    public async Task NetMQPush_ZmqSharpPull_Delivers()
    {
        await using var pull = new ZPullSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var token = TestContext.Current.CancellationToken;
        var port = InteropHelpers.GetFreePort();
        await pull.BindAsync($"tcp://127.0.0.1:{port}", token);

        using var push = new PushSocket();
        push.Options.Linger = TimeSpan.Zero;
        push.Connect($"tcp://127.0.0.1:{port}");

        for (var i = 0; i < 5; i++)
        {
            push.SendFrame(Encoding.ASCII.GetBytes($"push-{i}"));
            var message = await ReadMessageAsync(pull.Messages, TimeSpan.FromSeconds(5), token);
            Assert.NotNull(message);
            Assert.Equal(Encoding.ASCII.GetBytes($"push-{i}"), message.Value[0].ToSequence().ToArray());
            message.Value.Dispose();
        }
    }

    [Fact(Timeout = 20_000)]
    public async Task ZmqSharpPush_RoundRobinsAcrossTwoPulls()
    {
        using var pullA = new PullSocket();
        using var pullB = new PullSocket();
        pullA.Options.Linger = TimeSpan.Zero;
        pullB.Options.Linger = TimeSpan.Zero;
        var portA = InteropHelpers.GetFreePort();
        var portB = InteropHelpers.GetFreePort();
        pullA.Bind($"tcp://127.0.0.1:{portA}");
        pullB.Bind($"tcp://127.0.0.1:{portB}");

        await using var push = new ZPushSocket();
        var token = TestContext.Current.CancellationToken;
        await push.ConnectAsync($"tcp://127.0.0.1:{portA}", token);
        await push.ConnectAsync($"tcp://127.0.0.1:{portB}", token);

        // Distinct payloads per turn make cross-peer reordering detectable.
        for (var i = 0; i < 8; i++)
            await push.SendAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"turn-{i}")), token);

        // Drain both pulls until all eight turns arrive or the overall window
        // expires: the messages may still be in flight on a slow runner, so a
        // single bounded drain would race them. The round-robin cursor
        // alternates peers, so each pull must end with exactly four messages.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        var turnsA = new List<string>();
        var turnsB = new List<string>();
        while (turnsA.Count + turnsB.Count < 8 && DateTime.UtcNow < deadline)
        {
            turnsA.AddRange(DrainAvailable(pullA));
            turnsB.AddRange(DrainAvailable(pullB));
            await Task.Delay(20, token);
        }

        Assert.Equal(4, turnsA.Count);
        Assert.Equal(4, turnsB.Count);
        Assert.All(turnsA, turn => Assert.True(int.Parse(turn.Substring(5)) % 2 == 0, $"Expected '{turn}' to carry an even turn index."));
        Assert.All(turnsB, turn => Assert.True(int.Parse(turn.Substring(5)) % 2 == 1, $"Expected '{turn}' to carry an odd turn index."));
    }

    private static IEnumerable<string> DrainAvailable(PullSocket pull)
    {
        while (pull.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(50), out var frame))
            yield return Encoding.ASCII.GetString(frame);
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
}
