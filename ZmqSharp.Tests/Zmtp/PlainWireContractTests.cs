using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Zmtp;
using InteropHelpers = ZmqSharp.Tests.Interop.InteropHelpers;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// PLAIN wire-contract tests (0016 milestone 3): a scripted raw-TCP peer
/// speaks RFC 27 exactly as libzmq would, so the PLAIN bytes our library
/// sends and accepts are verified byte-for-byte against the specification.
/// NetMQ implements no PLAIN mechanism - 4.0.4.3 and master both error at the
/// greeting ("Not yet supported"), with no PlainServer/PlainUsername options -
/// so no NetMQ PLAIN peer exists to interop against. The ZMTP framing and
/// command layers these tests build on are already locked by the NetMQ NULL
/// interop suite; the HELLO/WELCOME/ERROR bodies and the as-server bit are
/// constructed here independently from the RFC.
/// </summary>
[Trait(InteropHelpers.InteropCategory, "true")]
public sealed class PlainWireContractTests
{
    [Fact]
    public async Task ClientHello_WireBytes_MatchRfc27()
    {
        // Our PLAIN client vs. a scripted libzmq-style PLAIN server.
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new ZPlainMechanism("alice", "s3cret"u8) },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var port = GetFreePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var connectTask = client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        using var raw = await listener.AcceptTcpClientAsync(cts.Token);
        listener.Stop();
        var stream = raw.GetStream();

        // The client's greeting advertises PLAIN with as-server = 0.
        AssertGreeting(await ReadExactlyAsync(stream, 64, cts.Token), "PLAIN", false);

        // The scripted PLAIN server greets back with as-server = 1.
        await stream.WriteAsync(BuildGreeting("PLAIN", true), cts.Token);

        // The client's HELLO must be byte-exact RFC 27: the short-string name
        // plus Username and Password metadata properties.
        var hello = await ReadFrameAsync(stream, cts.Token);
        hello.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeTrue();
        hello.Body.Should().Equal(ExpectedHelloBody("alice", "s3cret"));

        // WELCOME, then the client's READY carrying Socket-Type. The frame
        // body is [name-len]["READY"][metadata]; the metadata parser takes
        // only the property part, so strip the command name first.
        await stream.WriteAsync(BuildFrame(ExpectedWelcomeBody(), true), cts.Token);
        var ready = await ReadFrameAsync(stream, cts.Token);
        ready.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeTrue();
        ParseReadySocketType(ready.Body).Should().Be("PAIR");

        // Complete the handshake, then exchange a data frame.
        await stream.WriteAsync(BuildFrame(ZmtpCommands.BuildReady("PAIR"), true), cts.Token);
        await connectTask.WaitAsync(cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hi"u8]), cts.Token);
        var data = await ReadFrameAsync(stream, cts.Token);
        data.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeFalse();
        data.Body.Should().Equal([.. "hi"u8]);
    }

    [Fact]
    public async Task ServerWelcomeAndReady_WireBytes_MatchRfc27()
    {
        // Our PLAIN server vs. a scripted libzmq-style PLAIN client.
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("s3cret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port, cts.Token);
        var stream = raw.GetStream();

        // The server's greeting advertises PLAIN with as-server = 1.
        AssertGreeting(await ReadExactlyAsync(stream, 64, cts.Token), "PLAIN", true);

        // The scripted PLAIN client greets back (as-server = 0) and sends
        // HELLO with the exact RFC 27 bytes.
        await stream.WriteAsync(BuildGreeting("PLAIN", false), cts.Token);
        await stream.WriteAsync(BuildFrame(ExpectedHelloBody("alice", "s3cret"), true), cts.Token);

        // The server's WELCOME must be byte-exact RFC 27.
        var welcome = await ReadFrameAsync(stream, cts.Token);
        welcome.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeTrue();
        welcome.Body.Should().Equal(ExpectedWelcomeBody());

        // The server's READY carries Socket-Type.
        var ready = await ReadFrameAsync(stream, cts.Token);
        ready.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeTrue();
        ParseReadySocketType(ready.Body).Should().Be("PAIR");

        // Complete the handshake, then exchange a data frame.
        await stream.WriteAsync(BuildFrame(ZmtpCommands.BuildReady("PAIR"), true), cts.Token);
        await stream.WriteAsync(BuildFrame([.. "yo"u8], false), cts.Token);

        var message = await ReadMessageAsync(server.Messages, TimeSpan.FromSeconds(5), cts.Token);
        message.Should().NotBeNull();
        message.Value[0].ToSequence().ToArray().Should().Equal([.. "yo"u8]);
        message.Value.Dispose();
    }

    [Fact]
    public async Task ServerRejection_ErrorBytes_MatchRfc27()
    {
        // Rejected credentials: the server answers with libzmq's exact ERROR
        // bytes and tears the connection down - no WELCOME follows.
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new ZPlainMechanism((user, pass) =>
                    user == "alice" && pass.SequenceEqual("s3cret"u8))
            },
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true },
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var port = GetFreePort();
        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port, cts.Token);
        var stream = raw.GetStream();

        await ReadExactlyAsync(stream, 64, cts.Token); // server greeting
        await stream.WriteAsync(BuildGreeting("PLAIN", false), cts.Token);
        await stream.WriteAsync(BuildFrame(ExpectedHelloBody("alice", "wrong"), true), cts.Token);

        var error = await ReadFrameAsync(stream, cts.Token);
        error.Flags.HasFlag(ZmtpFrameFlags.Command).Should().BeTrue();
        error.Body.Should().Equal(ExpectedErrorBody("Invalid username or password"));

        // The server closes the connection after the rejection.
        var buffer = new byte[1];
        (await stream.ReadAsync(buffer, cts.Token)).Should().Be(0);
    }

    // ---- Independent RFC 27 fixtures (built from the spec, not the library) ----

    /// <summary>Reads Socket-Type from a full READY frame body (name prefix + metadata).</summary>
    private static string ParseReadySocketType(byte[] body)
    {
        var span = body.AsSpan();
        ZmtpCommandCodec.TryReadCommandName(span, out var name).Should().BeTrue();
        name.SequenceEqual("READY"u8).Should().BeTrue();
        return ZmtpCommandCodec.ParseReadySocketType(span[(1 + name.Length)..]);
    }

    private static void AssertGreeting(byte[] greeting, string mechanism, bool asServer)
    {
        greeting[0].Should().Be(0xFF);
        greeting[9].Should().Be(0x7F);
        greeting[10].Should().Be(3);
        Encoding.ASCII.GetString(greeting, 12, 20).TrimEnd('\0').Should().Be(mechanism);
        greeting[32].Should().Be(asServer ? (byte)1 : (byte)0);
    }

    private static byte[] BuildGreeting(string mechanism, bool asServer)
    {
        var greeting = new byte[64];
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;
        Encoding.ASCII.GetBytes(mechanism).CopyTo(greeting.AsSpan(12));
        greeting[32] = asServer ? (byte)1 : (byte)0;
        return greeting;
    }

    /// <summary>HELLO body per RFC 27: name, Username property, Password property.</summary>
    private static byte[] ExpectedHelloBody(string username, string password)
    {
        var body = new List<byte> { 5 };
        body.AddRange("HELLO"u8);
        AppendMetadataProperty(body, "Username", username);
        AppendMetadataProperty(body, "Password", password);
        return [.. body];
    }

    /// <summary>WELCOME body per RFC 27: name only.</summary>
    private static byte[] ExpectedWelcomeBody()
    {
        var body = new byte[8];
        body[0] = 7;
        "WELCOME"u8.CopyTo(body.AsSpan(1));
        return body;
    }

    /// <summary>ERROR body with the libzmq standard rejection reason.</summary>
    private static byte[] ExpectedErrorBody(string reason)
    {
        var reasonBytes = Encoding.ASCII.GetBytes(reason);
        var body = new byte[1 + 5 + 1 + reasonBytes.Length];
        body[0] = 5;
        "ERROR"u8.CopyTo(body.AsSpan(1));
        body[6] = (byte)reasonBytes.Length;
        reasonBytes.CopyTo(body.AsSpan(7));
        return body;
    }

    private static void AppendMetadataProperty(List<byte> target, string name, string value)
    {
        var nameBytes = Encoding.ASCII.GetBytes(name);
        var valueBytes = Encoding.UTF8.GetBytes(value);
        target.Add((byte)nameBytes.Length);
        target.AddRange(nameBytes);
        var length = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, valueBytes.Length);
        target.AddRange(length);
        target.AddRange(valueBytes);
    }

    // ---- Frame plumbing for the scripted peer ----

    private static byte[] BuildFrame(byte[] body, bool command)
    {
        var isLong = body.Length > 255;
        var frame = new byte[(isLong ? 9 : 2) + body.Length];
        var flags = command ? ZmtpFrameFlags.Command : ZmtpFrameFlags.None;
        if (isLong) flags |= ZmtpFrameFlags.LongSize;
        frame[0] = (byte)flags;
        if (isLong) BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(1), body.Length);
        else frame[1] = (byte)body.Length;
        body.CopyTo(frame.AsSpan(isLong ? 9 : 2));
        return frame;
    }

    private static async Task<(byte[] Body, ZmtpFrameFlags Flags)> ReadFrameAsync(Stream stream,
        CancellationToken token)
    {
        var flags = (ZmtpFrameFlags)(await ReadExactlyAsync(stream, 1, token))[0];
        var isLong = flags.HasFlag(ZmtpFrameFlags.LongSize);
        var sizeBytes = await ReadExactlyAsync(stream, isLong ? 8 : 1, token);
        var size = isLong ? BinaryPrimitives.ReadInt64BigEndian(sizeBytes) : sizeBytes[0];
        var body = await ReadExactlyAsync(stream, (int)size, token);
        return (body, flags);
    }

    private static async Task<byte[]> ReadExactlyAsync(Stream stream, int count, CancellationToken token)
    {
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, token);
        return buffer;
    }

    // ---- Test helpers ----

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
