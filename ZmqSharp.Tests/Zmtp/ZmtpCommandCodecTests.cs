using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// READY identity handling (0025): <see cref="ZmtpCommands.BuildReady"/>
/// attaches the Identity metadata property for router-addressable types, and
/// <see cref="ZmtpCommandCodec.ParseReadyIdentity"/> reads it back as raw
/// bytes.
/// </summary>
public sealed class ZmtpCommandCodecTests
{
    [Fact]
    public void BuildReady_WithoutIdentity_IsUnchangedAndHasNoIdentityProperty()
    {
        var body = ZmtpCommands.BuildReady("DEALER");

        Assert.Equal("DEALER", ZmtpCommandCodec.ParseReadySocketType(MetadataOf(body)));
        Assert.Null(ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body)));
    }

    [Fact]
    public void BuildReady_WithIdentity_RoundTripsRawBytes()
    {
        // A Guid-shaped identity is opaque bytes: not valid UTF-8 and with a
        // leading 0x00, which must survive the wire untouched.
        byte[] identity = [0x00, 0x01, 0xFF, 0x42, 0x13, 0x37];

        var body = ZmtpCommands.BuildReady("DEALER", identity);

        var parsed = ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body));
        Assert.NotNull(parsed);
        Assert.Equal(identity, parsed.Value.ToArray());
        Assert.Equal("DEALER", ZmtpCommandCodec.ParseReadySocketType(MetadataOf(body)));
    }

    [Fact]
    public void ParseReadyIdentity_NullOnAbsentAndEmpty()
    {
        Assert.Null(ZmtpCommandCodec.ParseReadyIdentity([]));

        // A default identity builds the same READY as today: no Identity property.
        var empty = ZmtpCommands.BuildReady("DEALER");
        Assert.Null(ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(empty)));
    }

    [Fact]
    public void ParseReadyIdentity_IsOpaque_AndLeavesOtherPropertiesAlone()
    {
        var identity = new byte[] { 0xDE, 0xAD };
        var body = ZmtpCommands.BuildReady("ROUTER", identity);

        // The string property view still sees Socket-Type; the raw identity
        // path returns the bytes untouched.
        var metadata = ZmtpCommandCodec.ParseMetadata(MetadataOf(body));
        Assert.Contains("Socket-Type", metadata);
        Assert.Equal("ROUTER", metadata["Socket-Type"]);

        var parsed = ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body));
        Assert.NotNull(parsed);
        Assert.Equal(identity, parsed.Value.ToArray());
    }

    [Fact]
    public void ParseReadyIdentity_RejectsDuplicateIdentityProperty()
    {
        var body = BuildReadyWithDuplicateIdentity();

        var ex = Assert.Throws<ZeroMqProtocolException>(
            () => ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body)));
        Assert.Contains("duplicate metadata property 'Identity'", ex.Message);
    }

    /// <summary>Strips the READY command-name prefix, leaving the metadata arguments.</summary>
    private static ReadOnlySpan<byte> MetadataOf(byte[] readyBody)
    {
        var nameLength = readyBody[0];
        return readyBody.AsSpan(1 + nameLength);
    }

    /// <summary>Builds a READY body whose metadata repeats the Identity property.</summary>
    private static byte[] BuildReadyWithDuplicateIdentity()
    {
        var nameLength = "READY".Length;
        var socketTypeProperty = ZmtpCommandCodec.MetadataPropertyLength("Socket-Type".Length, "DEALER".Length);
        var identityProperty = ZmtpCommandCodec.MetadataPropertyLength("Identity".Length, 1);
        var body = new byte[1 + nameLength + socketTypeProperty + 2 * identityProperty];
        var span = body.AsSpan();
        span[0] = (byte)nameLength;
        "READY"u8.CopyTo(span[1..]);
        var offset = 1 + nameLength;
        offset += ZmtpCommandCodec.WriteMetadataProperty(span[offset..], "Socket-Type"u8, "DEALER"u8);
        offset += ZmtpCommandCodec.WriteMetadataProperty(span[offset..], "Identity"u8, [0x01]);
        ZmtpCommandCodec.WriteMetadataProperty(span[offset..], "Identity"u8, [0x02]);
        return body;
    }
}
