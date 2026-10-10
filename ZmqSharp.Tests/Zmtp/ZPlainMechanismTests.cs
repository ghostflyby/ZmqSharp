using System.Buffers;
using System.Text;
using System.Threading.Channels;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// PLAIN mechanism tests (0016 milestone 2, RFC 24): wire-fixture handshake
/// sequences through <see cref="ZmtpHandshake"/>, plus end-to-end handshakes
/// over real sockets. The library implementation uses only the public
/// mechanism surface (0016 section 3.1); an equivalent mechanism was verified
/// to compile in a separate no-InternalsVisibleTo probe project.
/// </summary>
public sealed class ZPlainMechanismTests
{
    private const long MaxCommandSize = ZmtpParser.DefaultMaxCommandSize;

    [Fact]
    public async Task Client_CompletesHandshake_WithWelcomeThenReady()
    {
        // Server side of the wire: greeting + WELCOME + READY (no HELLO).
        var token = TestContext.Current.CancellationToken;
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), WelcomeFrame(), ZmtpTestData.Ready());
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism("alice", "secret"u8));

        var result = await handshake.EstablishAsync(token);

        Assert.NotNull(result);
        Assert.Null(result.Value.Codec);
        Assert.Equal("PAIR", ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span));
    }

    [Fact]
    public async Task Server_CompletesHandshake_WithAuthenticatedHello()
    {
        // Client side of the wire: greeting + HELLO(alice, secret) + INITIATE.
        var token = TestContext.Current.CancellationToken;
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "secret"), InitiateFrame());
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var result = await handshake.EstablishAsync(token);

        Assert.NotNull(result);
        Assert.Equal("PAIR", ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span));
    }

    [Fact]
    public async Task Server_RejectedHello_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "wrong"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var exception = await Assert.ThrowsAsync<ZMechanismException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("Invalid username or password", exception.Message);
    }

    [Fact]
    public async Task Server_HelloMissingPassword_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", null));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((_, _) => true));

        var exception = await Assert.ThrowsAsync<ZMechanismException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("missing Username or Password", exception.Message);
    }

    [Fact]
    public async Task Server_UnexpectedCommandInsteadOfHello_Throws()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), ZmtpTestData.Ready());
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((_, _) => true));

        var exception = await Assert.ThrowsAsync<ZMechanismException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("expected HELLO", exception.Message);
    }

    [Fact]
    public async Task Client_PeerErrorAfterHello_ThrowsWithPeerReason()
    {
        // The server rejects the HELLO: the client sees ERROR carrying the
        // standard reason with spaces (0x20-0x7E printable, 0016 section 8).
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), ZmtpTestData.Error("Invalid username or password"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism("alice", "wrong"u8));

        var exception = await Assert.ThrowsAsync<ZMechanismException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("Invalid username or password", exception.Message);
    }

    [Fact]
    public async Task Client_MechanismNameMismatch_Throws()
    {
        // The peer advertises NULL, the local mechanism is PLAIN: the greeting
        // match fails before any PLAIN command is exchanged.
        using var connection = new ZConnection(new ChunkedMemoryStream(ZmtpTestData.Greeting()));
        using var handshake = NewHandshake(connection, new ZPlainMechanism("alice", "secret"u8));

        var exception = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("does not match the configured mechanism 'PLAIN'", exception.Message);
    }

    [Fact]
    public void ServerConfiguration_SelectsServerRole()
    {
        var server = new ZPlainMechanism((_, _) => true);
        Assert.Equal(ZMechanismRole.Server, server.Role);
        Assert.NotNull(server.CreateSession());
    }

    [Fact]
    public void ClientConfiguration_SelectsClientRole()
    {
        var client = new ZPlainMechanism("alice", "secret"u8);
        Assert.Equal(ZMechanismRole.Client, client.Role);
        Assert.NotNull(client.CreateSession());
    }

    [Theory(Timeout = 15_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PlainMechanism_EndToEnd_CompletesHandshake_AndEchoes(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        await RunExchangeAsync(kind, reversed: false, token);
    }

    [Theory(Timeout = 15_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PlainMechanism_ClientBinds_ServerConnects(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        await RunExchangeAsync(kind, reversed: true, token);
    }

    private static async Task RunExchangeAsync(TransportKind kind, bool reversed, CancellationToken token)
    {
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "secret"u8) },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });

        var endpoint = TestTransports.GetEndpoint(kind);
        await (reversed ? client : server).BindAsync(endpoint, token);
        await (reversed ? server : client).ConnectAsync(endpoint, token);

        await client.SendAsync(ZMessage.FromOwned([.. "ping"u8]), token);
        var echo = await TryReadAsync(server.Messages, TimeSpan.FromSeconds(5), token);
        Assert.NotNull(echo);
        Assert.Equal([.. "ping"u8], echo.Value[0].ToSequence().ToArray());
        echo.Value.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PlainMechanism_BadPassword_FaultsClientConnect(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "wrong"u8) },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint, token);

        var exception = await Assert.ThrowsAsync<ZMechanismException>(() => client.ConnectAsync(endpoint, token));
        Assert.Contains("Invalid username or password", exception.Message);
    }

    [Fact]
    public async Task Server_PreservesBinaryPasswordBytes()
    {
        var token = TestContext.Current.CancellationToken;
        byte[] helloBody = [5, .. "HELLO"u8, 5, .. "alice"u8, 3, 0, 255, 128];
        var input = ZmtpTestData.Concat(ZmtpTestData.Greeting("PLAIN"),
            ZmtpTestData.Frame(helloBody, command: true), InitiateFrame());
        using var connection = new ZConnection(new ChunkedMemoryStream(input));
        var authenticated = false;
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, password) =>
        {
            Assert.Equal("alice", user);
            Assert.Equal([0, 255, 128], password.ToArray());
            authenticated = true;
            return true;
        }));
        Assert.NotNull(await handshake.EstablishAsync(token));
        Assert.True(authenticated);
    }

    [Fact]
    public void Credentials_EnforceWireOctetLengthIncludingUtf8Bytes()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZPlainMechanism(new string('é', 128), "x"u8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZPlainMechanism("alice", new byte[256]));
        Assert.Equal(ZMechanismRole.Client, new ZPlainMechanism(new string('a', 255), new byte[255]).Role);
    }

    // ---- Fixtures ----

    private static ZmtpHandshake NewHandshake(IZConnection connection, ZPlainMechanism mechanism)
    {
        return new ZmtpHandshake(
            connection,
            mechanism,
            ZmtpCommands.BuildReady("PAIR"),
            MaxCommandSize);
    }

    /// <summary>HELLO uses octet-length credentials; null omits the password length.</summary>
    private static byte[] HelloFrame(string username, string? password)
    {
        var user = Encoding.UTF8.GetBytes(username);
        var body = new List<byte> { 5 };
        body.AddRange("HELLO"u8);
        body.Add((byte)user.Length);
        body.AddRange(user);
        if (password is not { } value)
        {
            return ZmtpTestData.Frame([.. body], command: true);
        }

        var bytes = Encoding.UTF8.GetBytes(value);
        body.Add((byte)bytes.Length);
        body.AddRange(bytes);
        return ZmtpTestData.Frame([.. body], command: true);
    }

    private static byte[] InitiateFrame()
    {
        var metadata = ZmtpCommands.BuildReady("PAIR").AsSpan(6);
        return ZmtpTestData.Frame([8, .. "INITIATE"u8, .. metadata], command: true);
    }

    /// <summary>WELCOME frame: short-string name, no properties.</summary>
    private static byte[] WelcomeFrame()
    {
        var body = new byte[8];
        body[0] = 7;
        "WELCOME"u8.CopyTo(body.AsSpan(1));
        return ZmtpTestData.Frame(body, command: true);
    }

    /// <summary>Reads within a bounded window; an expiry surfaces as cancellation.</summary>
    private static async Task<ZMessage?> TryReadAsync(
        ChannelReader<ZMessage> reader,
        TimeSpan timeout,
        CancellationToken token)
    {
        using var window = CancellationTokenSource.CreateLinkedTokenSource(token);
        window.CancelAfter(timeout);
        if (await reader.WaitToReadAsync(window.Token)) return await reader.ReadAsync(window.Token);

        return null;
    }
}
