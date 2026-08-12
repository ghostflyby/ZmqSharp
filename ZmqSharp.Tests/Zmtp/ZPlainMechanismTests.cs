using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Messages;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// PLAIN mechanism tests (0016 milestone 2, RFC 27): wire-fixture handshake
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

        var result = await handshake.EstablishAsync(ZMechanismRole.Client);

        result.Should().NotBeNull();
        result.Value.SessionConnection.Should().BeSameAs(connection);
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody).Should().Be("PAIR");
    }

    [Fact]
    public async Task Server_CompletesHandshake_WithAuthenticatedHello()
    {
        // Client side of the wire: greeting + HELLO(alice, secret) + READY.
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "secret"), ZmtpTestData.Ready("PAIR"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var result = await handshake.EstablishAsync(ZMechanismRole.Server);

        result.Should().NotBeNull();
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody).Should().Be("PAIR");
    }

    [Fact]
    public async Task Server_RejectedHello_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", "wrong"));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) =>
            user == "alice" && pass.SequenceEqual("secret"u8)));

        var act = () => handshake.EstablishAsync(ZMechanismRole.Server).AsTask();
        await act.Should().ThrowAsync<ZMechanismException>().WithMessage("*Invalid username or password*");
    }

    [Fact]
    public async Task Server_HelloMissingPassword_ThrowsMechanismException()
    {
        var peerBytes = ZmtpTestData.Concat(
            ZmtpTestData.Greeting("PLAIN"), HelloFrame("alice", null));
        using var connection = new ZConnection(new ChunkedMemoryStream(peerBytes));
        using var handshake = NewHandshake(connection, new ZPlainMechanism((user, pass) => true));

        var act = () => handshake.EstablishAsync(ZMechanismRole.Server).AsTask();
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

        var act = () => handshake.EstablishAsync(ZMechanismRole.Server).AsTask();
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

        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();
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

        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*does not match the configured mechanism 'PLAIN'*");
    }

    [Fact]
    public void ClientRole_WithoutCredentials_Throws()
    {
        var server = new ZPlainMechanism((user, pass) => true);
        var act = () => server.CreateSession(ZMechanismRole.Client);
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ServerRole_WithoutAuthenticator_Throws()
    {
        var client = new ZPlainMechanism("alice", "secret"u8);
        var act = () => client.CreateSession(ZMechanismRole.Server);
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task PlainMechanism_EndToEnd_CompletesHandshake_AndEchoes()
    {
        await using var server = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            }
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "secret"u8) }
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
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("secret"u8))
            }
        });
        await using var client = ZSocket.CreatePair(new ZQueueSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "wrong"u8) }
        });

        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}");

        var act = async () => await client.ConnectAsync($"tcp://127.0.0.1:{port}");
        await act.Should().ThrowAsync<ZMechanismException>()
            .WithMessage("*Invalid username or password*");
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

    /// <summary>HELLO frame with Username/Password metadata; a null password omits the property.</summary>
    private static byte[] HelloFrame(string username, string? password)
    {
        var user = Encoding.UTF8.GetBytes(username);
        var properties = new List<byte>();
        AppendMetadataProperty(properties, "Username"u8, user);
        if (password is not null) AppendMetadataProperty(properties, "Password"u8, Encoding.UTF8.GetBytes(password));

        var body = new List<byte> { 5 };
        body.AddRange("HELLO"u8);
        body.AddRange(properties);
        return ZmtpTestData.Frame([.. body], command: true);
    }

    private static void AppendMetadataProperty(List<byte> target, ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        target.Add((byte)name.Length);
        target.AddRange(name.ToArray());
        var length = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, value.Length);
        target.AddRange(length);
        target.AddRange(value.ToArray());
    }

    /// <summary>WELCOME frame: short-string name, no properties.</summary>
    private static byte[] WelcomeFrame()
    {
        var body = new byte[8];
        body[0] = 7;
        "WELCOME"u8.CopyTo(body.AsSpan(1));
        return ZmtpTestData.Frame(body, command: true);
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
