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
    // RFC 7748 scalar-mult base vector: serverPublic is X25519(serverSecret).
    var serverSecret = Convert.FromHexString(
        "77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
    var serverPublic = Convert.FromHexString(
        "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
    var clientSecret = Convert.FromHexString(
        "5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");

    await using var server = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions { Mechanism = new CurveMechanism(serverSecret) },
        ReceiveQueueFactory = new BoundedChannelOptions(8) { SingleWriter = true },
    });
    await using var client = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions
        {
            Mechanism = new CurveMechanism(clientSecret, serverPublic)
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
