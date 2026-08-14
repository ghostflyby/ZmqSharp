using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
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
    [Fact]
    public async Task ZmqSharpPush_NetMQPull_Delivers()
    {
        using var pull = new PullSocket();
        pull.Options.Linger = TimeSpan.Zero;
        var port = InteropHelpers.GetFreePort();
        pull.Bind($"tcp://127.0.0.1:{port}");

        await using var push = new ZPushSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await push.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        for (var i = 0; i < 5; i++)
        {
            await push.SendAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"msg-{i}")), cts.Token);
            var received = InteropHelpers.ReceiveFrame(pull, TimeSpan.FromSeconds(5));
            received.Should().Equal(Encoding.ASCII.GetBytes($"msg-{i}"));
        }
    }

    [Fact]
    public async Task NetMQPush_ZmqSharpPull_Delivers()
    {
        await using var pull = new ZPullSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true } });
        var port = InteropHelpers.GetFreePort();
        await pull.BindAsync($"tcp://127.0.0.1:{port}");

        using var push = new PushSocket();
        push.Options.Linger = TimeSpan.Zero;
        push.Connect($"tcp://127.0.0.1:{port}");

        for (var i = 0; i < 5; i++)
        {
            push.SendFrame(Encoding.ASCII.GetBytes($"push-{i}"));
            var message = await ReadMessageAsync(pull.Messages, TimeSpan.FromSeconds(5));
            message.Should().NotBeNull();
            message.Value[0].ToSequence().ToArray().Should().Equal(Encoding.ASCII.GetBytes($"push-{i}"));
            message.Value.Dispose();
        }
    }

    [Fact]
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
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await push.ConnectAsync($"tcp://127.0.0.1:{portA}", cts.Token);
        await push.ConnectAsync($"tcp://127.0.0.1:{portB}", cts.Token);

        // Distinct payloads per turn make cross-peer reordering detectable.
        for (var i = 0; i < 8; i++)
            await push.SendAsync(ZMessage.FromOwned(Encoding.ASCII.GetBytes($"turn-{i}")), cts.Token);

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
            await Task.Delay(20, cts.Token);
        }

        turnsA.Should().HaveCount(4);
        turnsB.Should().HaveCount(4);
        turnsA.Should().OnlyContain(turn => int.Parse(turn.Substring(5)) % 2 == 0);
        turnsB.Should().OnlyContain(turn => int.Parse(turn.Substring(5)) % 2 == 1);
    }

    private static IEnumerable<string> DrainAvailable(PullSocket pull)
    {
        while (pull.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(50), out var frame))
            yield return Encoding.ASCII.GetString(frame);
    }

    [Fact]
    public async Task Pull_SendThrows()
    {
        await using var pull = new ZPullSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true } });
        var act = () => pull.SendAsync(ZMessage.FromOwned([.. "x"u8])).AsTask();
        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("receive-only");
    }

    private static async Task<ZMessage?> ReadMessageAsync(ChannelReader<ZMessage> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }
}
