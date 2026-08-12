using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Extensibility;

/// <summary>
/// End-to-end tests for a mechanism built only from public API (0016 section
/// 10): a PLAIN handshake completes over real sockets and a rejected
/// authentication faults the client's ConnectAsync with ZMechanismException.
/// </summary>
public sealed class PlainMechanismTests
{
    [Fact]
    public async Task PlainMechanism_CompletesHandshake_AndEchoes()
    {
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions
            {
                Mechanism = new PlainMechanism((user, pass) =>
                    user == "alice" && pass.Span.SequenceEqual("secret"u8))
            }
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new PlainMechanism("alice", "secret"u8) }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "ping"u8]), cts.Token);
        var echo = await TryReadAsync(server.Messages, TimeSpan.FromSeconds(5), cts.Token);
        echo.Should().NotBeNull();
        echo.Value[0].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        echo.Value.Dispose();
    }

    [Fact]
    public async Task PlainMechanism_BadPassword_FaultsClientConnect()
    {
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions
            {
                Mechanism = new PlainMechanism((user, pass) =>
                    user == "alice" && pass.Span.SequenceEqual("secret"u8))
            }
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new PlainMechanism("alice", "wrong"u8) }
        });

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        var act = async () => await client.ConnectAsync($"tcp://127.0.0.1:{port}");
        await act.Should().ThrowAsync<ZMechanismException>()
            .WithMessage("*Invalid username or password*");
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task<ZMessage?> TryReadAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan timeout,
        CancellationToken token)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(timeout);
        if (await reader.WaitToReadAsync(cts.Token)) return await reader.ReadAsync(cts.Token);

        return null;
    }
}
