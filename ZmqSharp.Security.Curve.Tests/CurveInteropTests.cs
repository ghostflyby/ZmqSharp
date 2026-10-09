using System.Buffers;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using NetMQ;
using NetMQ.Sockets;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

public sealed class CurveInteropTests
{
    [Theory(Timeout = 10_000)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task ExchangesAuthenticatedMessages_WithNetMQ(bool localServer, bool localBinds)
    {
        var server = new NetMQCertificate();
        var client = new NetMQCertificate();
        using var reference = new PairSocket();
        reference.Options.Linger = TimeSpan.Zero;
        reference.Options.CurveCertificate = localServer ? client : server;
        if (localServer) reference.Options.CurveServerKey = server.PublicKey;
        else reference.Options.CurveServer = true;
        var backend = new BouncyCastleCurveCrypto();
        var mechanism = localServer
            ? new CurveMechanism(backend, Key32.From(server.SecretKey))
            : new CurveMechanism(backend, Key32.From(client.SecretKey), Key32.From(server.PublicKey));
        await using var local = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = mechanism }
        });
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = $"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        listener.Stop();
        var token = TestContext.Current.CancellationToken;
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        void SendHello(object? sender, NetMQSocketEventArgs args)
        {
            if (args.Socket.TrySendFrame("reference-hello")) args.Socket.SendReady -= SendHello;
        }

        reference.SendReady += SendHello;
        reference.ReceiveReady += (_, args) =>
        {
            if (args.Socket.TryReceiveFrameBytes(out var bytes))
            {
                received.TrySetResult(bytes);
                args.Socket.TrySendFrame("reference-reply").Should().BeTrue();
            }
        };
        if (localBinds)
        {
            await local.BindAsync(endpoint, token);
            reference.Connect(endpoint);
        }
        else reference.Bind(endpoint);

        using var poller = new NetMQPoller();
        poller.Add(reference);
        poller.RunAsync();
        if (!localBinds) await local.ConnectAsync(endpoint, token);
        using (var hello = await local.Messages.ReadAsync(token))
            hello[0].ToSequence().ToArray().Should().Equal("reference-hello"u8.ToArray());
        await local.SendAsync("authenticated"u8.ToArray(), token);
        (await received.Task.WaitAsync(token)).Should().Equal("authenticated"u8.ToArray());
        using var reply = await local.Messages.ReadAsync(token);
        reply[0].ToSequence().ToArray().Should().Equal("reference-reply"u8.ToArray());
        poller.StopAsync();
    }
}
