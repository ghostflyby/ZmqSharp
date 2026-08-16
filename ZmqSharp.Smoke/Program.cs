using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using ZmqSharp;
using ZmqSharp.Security.Curve;

// Consumes the real ZmqSharp and ZmqSharp.Security.Curve packages (from nuget.org
// via the package references) the way a user would, then runs one loopback PAIR
// exchange and one CURVE exchange. The publish workflow runs this after publish to
// prove the shipped packages restore and work end to end, including the core/Curve
// dependency graph.
// Return codes: 0 = smoke passed, 1 = failed.

// --- Core package: loopback PAIR exchange over TCP. ---
{
    await using var server = new ZPairSocket();
    await using var client = new ZPairSocket();

    var port = GetFreePort();
    await server.BindAsync($"tcp://127.0.0.1:{port}");
    await client.ConnectAsync($"tcp://127.0.0.1:{port}");

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await client.SendAsync(ZMessage.FromOwned([.. "smoke-ok"u8]), cts.Token);
    var message = await server.Messages.ReadAsync(cts.Token);
    if (!message[0].ToSequence().ToArray().SequenceEqual("smoke-ok"u8.ToArray()))
    {
        Console.Error.WriteLine("SMOKE-FAIL: PAIR exchange received wrong payload");
        return 1;
    }
    message.Dispose();
}

// --- Curve package: loopback CURVE exchange over TCP. ---
{
    var crypto = new BouncyCastleCurveCrypto();
    crypto.GenerateKeyPair(out var serverPublic, out var serverSecret);
    crypto.GenerateKeyPair(out var clientPublic, out var clientSecret);

    await using var server = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions { Mechanism = new CurveMechanism(crypto, serverSecret) },
        ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
    });
    await using var client = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions
        {
            Mechanism = new CurveMechanism(crypto, clientSecret, serverPublic)
        },
        ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
    });

    var port = GetFreePort();
    await server.BindAsync($"tcp://127.0.0.1:{port}");
    await client.ConnectAsync($"tcp://127.0.0.1:{port}");

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await client.SendAsync(ZMessage.FromOwned([.. "curve-ok"u8]), cts.Token);
    var message = await server.Messages.ReadAsync(cts.Token);
    if (!message[0].ToSequence().ToArray().SequenceEqual("curve-ok"u8.ToArray()))
    {
        Console.Error.WriteLine("SMOKE-FAIL: CURVE exchange received wrong payload");
        return 1;
    }
    message.Dispose();
}

Console.WriteLine("SMOKE-OK");
return 0;

static int GetFreePort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
