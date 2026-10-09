using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
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
    private const int MaxCommandSize = ZmtpParser.DefaultMaxCommandSize;

    [Fact]
    public async Task Client_CompletesHandshake_WithWelcomeThenReady()
    {
        // Server side of the wire: greeting + WELCOME + READY (no HELLO).
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), WelcomeFrame(), ZmtpTestData.Ready("PAIR"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism("alice", "secret"u8));

        var result = await handshake.EstablishAsync();

        result.Should().NotBeNull();
        result.Value.Codec.Should().BeNull();
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span).Should().Be("PAIR");
    }

    [Fact]
    public async Task Server_CompletesHandshake_WithAuthenticatedHello()
    {
        // Client side of the wire: greeting + HELLO(alice, secret) + INITIATE.
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "secret"), InitiateFrame());
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var result = await handshake.EstablishAsync();

        result.Should().NotBeNull();
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span).Should().Be("PAIR");
    }

    [Fact]
    public async Task Server_RejectedHello_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "wrong"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var act = () => handshake.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZMechanismException>().WithMessage("*Invalid username or password*");
    }

    [Fact]
    public async Task Server_HelloMissingPassword_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", null));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) => true));

        var act = () => handshake.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZMechanismException>()
            .WithMessage("*missing Username or Password*");
    }

    [Fact]
    public async Task Server_UnexpectedCommandInsteadOfHello_Throws()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), ZmtpTestData.Ready("PAIR"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) => true));

        var act = () => handshake.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZMechanismException>().WithMessage("*expected HELLO*");
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

        var act = () => handshake.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZMechanismException>()
            .WithMessage("*Invalid username or password*");
    }

    [Fact]
    public async Task Client_MechanismNameMismatch_Throws()
    {
        // The peer advertises NULL, the local mechanism is PLAIN: the greeting
        // match fails before any PLAIN command is exchanged.
        using var connection = new ZConnection(new ChunkedMemoryStream(ZmtpTestData.Greeting()));
        using var handshake = NewHandshake(connection, new ZPlainMechanism("alice", "secret"u8));

        var act = () => handshake.EstablishAsync().AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*does not match the configured mechanism 'PLAIN'*");
    }

    [Fact]
    public void ServerConfiguration_SelectsServerRole()
    {
        var server = new ZPlainMechanism((user, pass) => true);
        server.Role.Should().Be(ZMechanismRole.Server);
        server.CreateSession().Should().NotBeNull();
    }

    [Fact]
    public void ClientConfiguration_SelectsClientRole()
    {
        var client = new ZPlainMechanism("alice", "secret"u8);
        client.Role.Should().Be(ZMechanismRole.Client);
        client.CreateSession().Should().NotBeNull();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public Task PlainMechanism_EndToEnd_CompletesHandshake_AndEchoes(TransportKind kind)
        => RunExchangeAsync(kind, false);

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public Task PlainMechanism_ClientBinds_ServerConnects(TransportKind kind)
        => RunExchangeAsync(kind, true);

    private static async Task RunExchangeAsync(TransportKind kind, bool reversed)
    {
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "secret"u8) },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var endpoint = TestTransports.GetEndpoint(kind);
        await (reversed ? client : server).BindAsync(endpoint, cts.Token);
        await (reversed ? server : client).ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "ping"u8]), cts.Token);
        var echo = await TryReadAsync(server.Messages, TimeSpan.FromSeconds(5), cts.Token);
        echo.Should().NotBeNull();
        echo.Value[0].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        echo.Value.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task PlainMechanism_BadPassword_FaultsClientConnect(TransportKind kind)
    {
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "wrong"u8) },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });

        var endpoint = TestTransports.GetEndpoint(kind);
        await server.BindAsync(endpoint);

        await FluentActions.Awaiting(() => client.ConnectAsync(endpoint)).Should().ThrowAsync<ZMechanismException>()
            .WithMessage("*Invalid username or password*");
    }

    [Fact]
    public async Task Server_PreservesBinaryPasswordBytes()
    {
        byte[] helloBody = [5, .. "HELLO"u8, 5, .. "alice"u8, 3, 0, 255, 128];
        var input = ZmtpTestData.Concat(ZmtpTestData.Greeting("PLAIN"),
            ZmtpTestData.Frame(helloBody, command: true), InitiateFrame());
        using var connection = new ZConnection(new ChunkedMemoryStream(input));
        var authenticated = false;
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, password) =>
        {
            user.Should().Be("alice");
            password.ToArray().Should().Equal(0, 255, 128);
            authenticated = true;
            return true;
        }));
        (await handshake.EstablishAsync()).Should().NotBeNull();
        authenticated.Should().BeTrue();
    }

    [Fact]
    public void Credentials_EnforceWireOctetLengthIncludingUtf8Bytes()
    {
        FluentActions.Invoking(() => new ZPlainMechanism(new string('é', 128), "x"u8))
            .Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => new ZPlainMechanism("alice", new byte[256]))
            .Should().Throw<ArgumentOutOfRangeException>();
        new ZPlainMechanism(new string('a', 255), new byte[255]).Role.Should().Be(ZMechanismRole.Client);
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
        if (password is { } value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            body.Add((byte)bytes.Length);
            body.AddRange(bytes);
        }

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
