using System.Buffers.Binary;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
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
    private const long MaxCommandSize = ZmtpParser.DefaultMaxCommandSize;

    private static ReadOnlySpan<byte> ReadyName => "READY"u8;

    [Fact]
    public void NullMechanism_IsRoleless_AndAdvertisesZero()
    {
        Assert.Equal(ZMechanismRole.None, ZNullMechanism.Instance.Role);
        Assert.Equal(0, ZmtpGreeting.Build("NULL", ZNullMechanism.Instance.Role)[32]);
    }

    [Fact]
    public async Task GreetingWithNullMechanism_Completes_AndYieldsPeerReady()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready()));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Null(result.Value.Codec);
        Assert.Equal("PAIR", ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span));
    }

    [Fact]
    public async Task PeerSocketType_ReadFromReadyMetadata_IsReturned()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Ready("DEALER")));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("DEALER", ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span));
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

        var result = await handshake.EstablishAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("CUSTOM", ZmtpCommandCodec.ParseReadySocketType(result.Value.PeerReadyBody.Span));
    }

    [Fact]
    public void ParseReadySocketType_MissingSocketType_Throws()
    {
        var body = ZmtpTestData.ReadyBodyWithProperties(("Identity", "abc"));

        var ex = Assert.Throws<ZeroMqProtocolException>(
            () => ZmtpCommandCodec.ParseReadySocketType(body.AsSpan()[(1 + ReadyName.Length)..]));

        Assert.Contains("missing a valid Socket-Type", ex.Message);
    }

    [Fact]
    public async Task ReadyWithAdditionalMetadata_Completes()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.ReadyWithProperties(("Socket-Type", "PAIR"), ("Identity", "abc"))));
        using var handshake = NewHandshake(connection);

        var result = await handshake.EstablishAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
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
        var ex = await Assert.ThrowsAsync<ZMechanismException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());

        Assert.Contains("boom", ex.Message);
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
        await AssertHandshakeCommandRejectedAsync([10, .. "RE"u8]);
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

        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
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
        Assert.Null(await handshake.EstablishAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CommandFrame_OnePastMaxCommandSize_ThrowsBeforeBodyRead()
    {
        using var connection = NewConnection(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), CommandFrameHeader(MaxCommandSize + 1)));
        using var handshake = NewHandshake(connection);

        var ex = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => handshake.EstablishAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("exceeds maximum size", ex.Message);
    }

    [Fact]
    public async Task PeerClosesAfterGreeting_ReturnsNull()
    {
        using var connection = NewConnection(ZmtpTestData.Greeting());
        using var handshake = NewHandshake(connection);

        Assert.Null(await handshake.EstablishAsync(TestContext.Current.CancellationToken));
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
        await Assert.ThrowsAnyAsync<ZeroMqProtocolException>(() => handshake.EstablishAsync().AsTask());
    }

    private static async Task AssertHandshakeCommandRejectedAsync(byte[] body)
    {
        await AssertEstablishmentRejectedAsync(ZmtpTestData.Concat(
            ZmtpTestData.Greeting(), ZmtpTestData.Frame(body, command: true)));
    }
}
