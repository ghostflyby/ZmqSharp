using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// Handshake tests (0016 section 10): greeting validation, mechanism matching,
/// and the NULL mechanism's command sequence, driven through
/// <see cref="ZmtpHandshake"/> directly. READY Socket-Type metadata
/// validation moved to the socket layer with the handshake boundary; those
/// cases live in ZSocketTests as ConnectAsync failures.
/// </summary>
public sealed class ZmtpHandshakeTests
{
    private const int MaxCommandSize = ZmtpParser.DefaultMaxCommandSize;

    private static ReadOnlySpan<byte> ReadyName => "READY"u8;

    [Fact]
    public async Task GreetingWithNullMechanism_Completes_AndYieldsPeerReady()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready()));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(ZMechanismRole.Client);

        result.Should().NotBeNull();
        result.Value.SessionConnection.Should().BeSameAs(connection);
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span).Should().Be("PAIR");
    }

    [Fact]
    public async Task PeerSocketType_ReadFromReadyMetadata_IsReturned()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("DEALER")));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(ZMechanismRole.Client);

        result.Should().NotBeNull();
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span).Should().Be("DEALER");
    }

    [Fact]
    public async Task PeerSocketType_UnknownName_IsReturnedByCodec()
    {
        // Custom socket types interoperate between ZmqSharp endpoints (0015
        // section 2.3): the codec must not reject an unknown Socket-Type -
        // acceptance is decided by the local socket's predicate, not the wire
        // codec.
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("CUSTOM")));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(ZMechanismRole.Client);

        result.Should().NotBeNull();
        ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span).Should().Be("CUSTOM");
    }

    [Fact]
    public void ParseReadySocketType_MissingSocketType_Throws()
    {
        var body = ZmtpTestData.ReadyBodyWithProperties(("Identity", "abc"));

        var act = () => ZmtpCommandCodec.ParseReadySocketType(body.AsSpan()[(1 + ReadyName.Length)..]);

        act.Should().Throw<ZeroMqProtocolException>().WithMessage("*missing a valid Socket-Type*");
    }

    [Fact]
    public async Task ReadyWithAdditionalMetadata_Completes()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithProperties(("Socket-Type", "PAIR"), ("Identity", "abc"))));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(ZMechanismRole.Client);

        result.Should().NotBeNull();
    }

    [Fact]
    public async Task BadGreetingSignature_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[0] = 0x00;
        await AssertEstablishmentRejectedAsync(greeting);
    }

    [Fact]
    public async Task UnsupportedVersion_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[10] = 2;
        await AssertEstablishmentRejectedAsync(greeting);
    }

    [Fact]
    public async Task PeerMechanismMismatch_Throws()
    {
        // The peer advertises CURVE but the local mechanism is NULL: the
        // configured instance is matched by name (0016 D1), never instantiated
        // from the wire string.
        await AssertEstablishmentRejectedAsync(ZmtpTestData.Greeting("CURVE"));
    }

    [Fact]
    public async Task EmptyGreetingMechanismName_Throws()
    {
        var greeting = ZmtpTestData.Greeting("");
        await AssertEstablishmentRejectedAsync(greeting);
    }

    [Fact]
    public async Task GreetingMechanismPaddingNotZeroFilled_Throws()
    {
        var greeting = ZmtpTestData.Greeting();
        greeting[13] = 0x01;
        await AssertEstablishmentRejectedAsync(greeting);
    }

    [Fact]
    public async Task ErrorCommandInHandshake_ThrowsWithPeerReason()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Error("boom")));
        using var handshake = NewHandshake(connection);
        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();

        await act.Should().ThrowAsync<ZMechanismException>().WithMessage("*boom*");
    }

    [Fact]
    public async Task UnknownCommandDuringHandshake_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([4, (byte)'P', (byte)'I', (byte)'N', (byte)'G']);
    }

    [Fact]
    public async Task CommandName_ZeroLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([0]);
    }

    [Fact]
    public async Task CommandName_Truncated_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([10, (byte)'R', (byte)'E']);
    }

    [Fact]
    public async Task CommandName_NonAlphabetic_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([1, (byte)'1']);
    }

    [Fact]
    public async Task CommandName_MissingLengthPrefix_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([.. "READY\0"u8]);
    }

    [Fact]
    public async Task DataFrameDuringHandshake_Throws()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Frame([.. "data"u8])));
        using var handshake = NewHandshake(connection);

        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ErrorCommand_EmptyReason_ThrowsProtocolException()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 0]);
    }

    [Fact]
    public async Task ErrorCommand_MissingReasonLength_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R']);
    }

    [Fact]
    public async Task ErrorCommand_ReasonLengthMismatch_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([
            5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 3, (byte)'x'
        ]);
    }

    [Fact]
    public async Task ErrorCommand_ReasonWithNonVisibleCharacter_Throws()
    {
        await AssertHandshakeCommandRejectedAsync([5, (byte)'E', (byte)'R', (byte)'R', (byte)'O', (byte)'R', 1, 0x08]);
    }

    [Fact]
    public async Task CommandFrame_AtMaxCommandSize_IsNotRejectedBySizeCheck()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), CommandFrameHeader(MaxCommandSize)));
        using var handshake = NewHandshake(connection);

        // No body follows the header, so the handshake ends at EOF; the size
        // check must not reject the boundary value itself.
        (await handshake.EstablishAsync(ZMechanismRole.Client)).Should().BeNull();
    }

    [Fact]
    public async Task CommandFrame_OnePastMaxCommandSize_ThrowsBeforeBodyRead()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), CommandFrameHeader(MaxCommandSize + 1)));
        using var handshake = NewHandshake(connection);

        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>().WithMessage("*exceeds maximum size*");
    }

    [Fact]
    public async Task PeerClosesAfterGreeting_ReturnsNull()
    {
        using var connection = NewConnection(ZmtpTestData.Greeting());
        using var handshake = NewHandshake(connection);

        (await handshake.EstablishAsync(ZMechanismRole.Client)).Should().BeNull();
    }

    private static ZConnection NewConnection(byte[] peerBytes)
    {
        return new ZConnection(new ChunkedMemoryStream(peerBytes));
    }

    private static ZmtpHandshake NewHandshake(IZConnection connection)
    {
        return new ZmtpHandshake(
            connection,
            ZNullMechanism.Instance,
            ZmtpCommands.BuildReady("PAIR"),
            MaxCommandSize);
    }

    private static byte[] CommandFrameHeader(long size)
    {
        var header = new byte[9];
        header[0] = (byte)(ZmtpFrameFlags.Command | ZmtpFrameFlags.LongSize);
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1), size);
        return header;
    }

    private static async Task AssertEstablishmentRejectedAsync(byte[] peerBytes)
    {
        using var connection = NewConnection(peerBytes);
        using var handshake = NewHandshake(connection);
        var act = () => handshake.EstablishAsync(ZMechanismRole.Client).AsTask();
        await act.Should().ThrowAsync<ZeroMqProtocolException>();
    }

    private static async Task AssertHandshakeCommandRejectedAsync(byte[] body)
    {
        await AssertEstablishmentRejectedAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Frame(body, command: true)));
    }
}
