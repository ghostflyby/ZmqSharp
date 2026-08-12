using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

/// <summary>
/// End-to-end CURVE tests: two ZmqSharp sockets configured with the optional
/// <see cref="CurveMechanism"/> authenticate and then exchange encrypted
/// messages over TCP. The package's default BouncyCastle backend is used, but
/// the mechanism composes any <see cref="ICurveCryptoBackend"/> - swapping the
/// backend is the only change needed to use a different crypto library.
/// </summary>
public sealed class CurveEndToEndTests
{
    [Fact]
    public async Task CurveClient_AndServer_AuthenticateAndExchangeMessages()
    {
        var crypto = new BouncyCastleCurveCrypto();
        var serverKeys = crypto.GenerateKeyPair();
        var clientKeys = crypto.GenerateKeyPair();

        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(crypto, serverKeys) }
        });

        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
            Security = new ZSecurityOptions
            {
                Mechanism = new CurveMechanism(crypto, clientKeys, serverKeys.PublicKey)
            }
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        // Client -> server.
        await client.SendAsync(ZMessage.FromOwned([.. "hello-secret"u8]), cts.Token);
        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), cts.Token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "hello-secret"u8]);
        message.Value.Dispose();

        // Server -> client (two frames; multipart construction is an internal
        // concern, so separate single-frame messages exercise the same seal path).
        await server.SendAsync(ZMessage.FromOwned([.. "a"u8]), cts.Token);
        await server.SendAsync(ZMessage.FromOwned([.. "b"u8]), cts.Token);
        var first = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), cts.Token);
        first.Should().NotBeNull();
        first.Value[0].ToSequence().ToArray().Should().Equal([.. "a"u8]);
        first.Value.Dispose();
        var second = await ReadMessageAsync(client.Messages, TimeSpan.FromSeconds(5), cts.Token);
        second.Should().NotBeNull();
        second.Value[0].ToSequence().ToArray().Should().Equal([.. "b"u8]);
        second.Value.Dispose();
    }

    [Fact]
    public async Task CurveClient_WithWrongServerKey_FailsHandshake()
    {
        var crypto = new BouncyCastleCurveCrypto();
        var serverKeys = crypto.GenerateKeyPair();
        var clientKeys = crypto.GenerateKeyPair();
        var wrongServerKeys = crypto.GenerateKeyPair();

        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new CurveMechanism(crypto, serverKeys) }
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions
            {
                // The client holds a different server public key: the WELCOME
                // box never opens, and establishment must fault.
                Mechanism = new CurveMechanism(crypto, clientKeys, wrongServerKeys.PublicKey)
            }
        });

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        // The client seals HELLO under the wrong server public key, so the
        // server's HELLO box never opens and it tears the connection down;
        // the client surfaces either the protocol failure or the peer close
        // (the same teardown race the socket layer documents).
        var act = async () => await client.ConnectAsync($"tcp://127.0.0.1:{port}");
        var failure = await Record.ExceptionAsync(act);
        failure.Should().NotBeNull();
        (failure is ZMechanismException or IOException).Should().BeTrue();
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
        catch (OperationCanceledException)
        {
            return null;
        }
    }
}
