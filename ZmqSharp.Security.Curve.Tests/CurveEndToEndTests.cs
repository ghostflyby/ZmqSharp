using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

/// <summary>
/// End-to-end CURVE tests: two ZmqSharp sockets configured with the
/// internal <see cref="CurveMechanism"/> authenticate and then exchange
/// encrypted messages over TCP. The in-box BouncyCastle primitives are used;
/// per RFC 25 the primitives are fixed on the wire, so a different crypto
/// library would ship as a byte-equivalent port, not a swappable backend.
/// </summary>
public sealed class CurveEndToEndTests
{
    [Theory(Timeout = 10_000)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task CurveClient_AndServer_AuthenticateAndExchangeMessages(bool reversed, bool ipc)
    {
        var token = TestContext.Current.CancellationToken;
        CurveCrypto.GenerateKeyPair(out var serverPublic, out var serverSecret);
        CurveCrypto.GenerateKeyPair(out var clientPublic, out var clientSecret);
        var serverKeys = (Public: serverPublic, Secret: serverSecret);
        var clientKeys = (Public: clientPublic, Secret: clientSecret);

        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(serverKeys.Secret) },
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });

        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new CurveMechanism(clientKeys.Secret, serverKeys.Public)
            },
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });
        var path = Path.Combine(Path.GetTempPath(), $"zmq-curve-{Guid.NewGuid().ToString("N")[..12]}.sock");
        var address = ipc ? $"ipc://{path}" : $"tcp://127.0.0.1:{GetFreePort()}";
        await (reversed ? client : server).BindAsync(address, token);
        await (reversed ? server : client).ConnectAsync(address, token);

        // Client -> server.
        await client.SendAsync(ZMessage.FromOwned([.. "hello-secret"u8]), token);
        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(message);
        Assert.Equal([.. "hello-secret"u8], message.Value[0].ToSequence().ToArray());
        message.Value.Dispose();

        // Server -> client (two frames; multipart construction is an internal
        // concern, so separate single-frame messages exercise the same seal path).
        await server.SendAsync(ZMessage.FromOwned([.. "a"u8]), token);
        await server.SendAsync(ZMessage.FromOwned([.. "b"u8]), token);
        var first = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(first);
        Assert.Equal([.. "a"u8], first.Value[0].ToSequence().ToArray());
        first.Value.Dispose();
        var second = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(second);
        Assert.Equal([.. "b"u8], second.Value[0].ToSequence().ToArray());
        second.Value.Dispose();
    }

    [Fact]
    public async Task CurveClient_WithWrongServerKey_FailsHandshake()
    {
        var token = TestContext.Current.CancellationToken;
        CurveCrypto.GenerateKeyPair(out _, out var serverSecret);
        CurveCrypto.GenerateKeyPair(out _, out var clientSecret);
        CurveCrypto.GenerateKeyPair(out var wrongPublic, out _);

        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(serverSecret) },
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true }
        });

        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                // The client holds a different server public key: the WELCOME
                // box never opens, and establishment must fault.
                Mechanism = new CurveMechanism(clientSecret, wrongPublic)
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", token);

        // The client seals HELLO under the wrong server public key, so the
        // server's HELLO box never opens, and it tears the connection down;
        // the client surfaces either the protocol failure or the peer close
        // (the same teardown race the socket layer documents).
        var failure = await Record.ExceptionAsync(async () => await client.ConnectAsync($"tcp://127.0.0.1:{port}", token));
        Assert.NotNull(failure);
        Assert.True(
            failure is ZMechanismException or IOException,
            $"Expected ZMechanismException or IOException, but was: {failure.GetType().FullName}");
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
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
