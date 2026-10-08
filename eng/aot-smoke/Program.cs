using System.Buffers;
using System.Net;
using System.Net.Sockets;
using ZmqSharp;
using ZmqSharp.Security;
using ZmqSharp.Security.Curve;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
if (args is ["plain-peer", var role, var topology, var peerAddress])
{
    await using var peer = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions
        {
            Mechanism = role == "server"
            ? new ZPlainMechanism((user, password) => user == "alice" && password.SequenceEqual("secret"u8))
            : new ZPlainMechanism("alice", "secret"u8)
        }
    });
    peer.PeerEnded += (_, failure) => Console.Error.WriteLine($"PLAIN peer ended: {failure}");
    if (topology == "bind")
    {
        await peer.BindAsync(peerAddress, timeout.Token);
        Console.WriteLine("BOUND");
    }
    else await peer.ConnectAsync(peerAddress, timeout.Token);
    using var incoming = await peer.Messages.ReadAsync(timeout.Token);
    if (!incoming[0].ToSequence().ToArray().AsSpan().SequenceEqual("reference"u8)) return 1;
    await peer.SendAsync("verified"u8.ToArray(), timeout.Token);
    return 0;
}

foreach (var ipc in (bool[])[false, true])
{
    // External assembly: only public mechanism/codec and byte contracts are available.
    await using var server = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions { Mechanism = new PublicMechanism() }
    });
    await using var client = new ZPairSocket();
    var address = Endpoint(ipc);
    await server.BindAsync(address, timeout.Token);
    var resolved = Resolve(address);
    await client.ConnectAsync<EndPoint, ByteTransport>(resolved, timeout.Token);
    await client.SendAsync("public-codec"u8.ToArray(), timeout.Token);
    using var message = await server.Messages.ReadAsync(timeout.Token);
    if (!message[0].ToSequence().ToArray().AsSpan().SequenceEqual("public-codec"u8)) return 1;
    ZPeer? ended = null;
    client.PeerEnded += (ZPeer peer, Exception? _) => ended = peer;
    await client.DisconnectAsync<EndPoint, ByteTransport>(resolved, timeout.Token);
    if (ended is null || ended.Id <= 0) return 1;
    await server.UnbindAsync(address, timeout.Token);

    var backend = new BouncyCastleCurveCrypto();
    backend.GenerateKeyPair(out var serverPublic, out var serverSecret);
    backend.GenerateKeyPair(out _, out var clientSecret);
    // Reverse topology: configured security client binds, configured server connects.
    await using var curveServer = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions { Mechanism = new CurveMechanism(backend, serverSecret) }
    });
    await using var curveClient = new ZPairSocket(new ZSocketOptions
    {
        Security = new ZSecurityOptions { Mechanism = new CurveMechanism(backend, clientSecret, serverPublic) }
    });
    var curveAddress = Endpoint(ipc);
    await curveClient.BindAsync(curveAddress, timeout.Token);
    await curveServer.ConnectAsync(curveAddress, timeout.Token);
    await curveServer.SendAsync("aot-curve"u8.ToArray(), timeout.Token);
    using var reply = await curveClient.Messages.ReadAsync(timeout.Token);
    if (!reply[0].ToSequence().ToArray().AsSpan().SequenceEqual("aot-curve"u8)) return 1;
    await curveServer.DisconnectAsync(curveAddress, timeout.Token);
    await curveClient.UnbindAsync(curveAddress, timeout.Token);
}
Console.WriteLine("AOT-SMOKE-OK");
return 0;

static string Endpoint(bool ipc)
{
    if (ipc) return $"ipc://{Path.Combine(Path.GetTempPath(), $"zmq-aot-{Guid.NewGuid().ToString("N")[..12]}.sock")}";
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    return $"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
}
static EndPoint Resolve(string address)
    => address.StartsWith("ipc://", StringComparison.Ordinal)
        ? new UnixDomainSocketEndPoint(address[6..])
        : new IPEndPoint(IPAddress.Loopback, int.Parse(address[(address.LastIndexOf(':') + 1)..]));

sealed class PublicMechanism : IZSecurityMechanism
{
    public string Name => "NULL";
    public ZMechanismRole Role => ZMechanismRole.None;
    public IZMechanismSession CreateSession() => new Session();
    private sealed class Session : IZMechanismSession
    {
        public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
        {
            var result = await ZNullMechanism.Instance.CreateSession().RunAsync(context, token);
            return result is { } ready ? new ZMechanismResult(new IdentityCodec(), ready.PeerReadyBody) : null;
        }
    }
    private sealed class IdentityCodec : IZFrameCodec
    {
        public ZmtpFrameData Encode(ZmtpFrameData input) => input;
        public ZmtpFrameData Decode(ZmtpFrameData input) => input;
        public long GetMaximumEncodedLength(long length) => length;
        public void Dispose() { }
    }
}

sealed class ByteTransport : IZTransport<ByteTransport, EndPoint>
{
    public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept { add { } remove { } }
    public static async ValueTask<IZConnection> ConnectAsync(EndPoint endpoint, CancellationToken token = default)
        => new Bytes(await SocketTransport.ConnectAsync(endpoint, token));
    public static ValueTask<ByteTransport> BindAsync(EndPoint endpoint, CancellationToken token = default)
        => throw new NotSupportedException();
    public ValueTask StartAsync(CancellationToken token = default) => throw new NotSupportedException();
    public void Dispose() { }
    private sealed class Bytes(IZConnection raw) : IZConnection
    {
        public ValueTask<int> ReadAsync(Memory<byte> target, CancellationToken token = default) => raw.ReadAsync(target, token);
        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default) => raw.WriteAsync(bytes, token);
        public ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default) => raw.WriteAsync(bytes, token);
        public void Abort() => raw.Abort();
        public void Dispose() => raw.Dispose();
    }
}
