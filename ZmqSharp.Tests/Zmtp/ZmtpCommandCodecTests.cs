using FluentAssertions;
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

        ZmtpCommandCodec.ParseReadySocketType(MetadataOf(body)).Should().Be("DEALER");
        ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body)).Should().BeNull();
    }

    [Fact]
    public void BuildReady_WithIdentity_RoundTripsRawBytes()
    {
        // A Guid-shaped identity is opaque bytes: not valid UTF-8 and with a
        // leading 0x00, which must survive the wire untouched.
        byte[] identity = [0x00, 0x01, 0xFF, 0x42, 0x13, 0x37];

        var body = ZmtpCommands.BuildReady("DEALER", identity);

        var parsed = ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body));
        parsed.Should().NotBeNull();
        parsed.Value.ToArray().Should().Equal(identity);
        ZmtpCommandCodec.ParseReadySocketType(MetadataOf(body)).Should().Be("DEALER");
    }

    [Fact]
    public void ParseReadyIdentity_NullOnAbsentAndEmpty()
    {
        ZmtpCommandCodec.ParseReadyIdentity([]).Should().BeNull();

        // A default identity builds the same READY as today: no Identity property.
        var empty = ZmtpCommands.BuildReady("DEALER");
        ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(empty)).Should().BeNull();
    }

    [Fact]
    public void ParseReadyIdentity_IsOpaque_AndLeavesOtherPropertiesAlone()
    {
        var identity = new byte[] { 0xDE, 0xAD };
        var body = ZmtpCommands.BuildReady("ROUTER", identity);

        // The string property view still sees Socket-Type; the raw identity
        // path returns the bytes untouched.
        var metadata = ZmtpCommandCodec.ParseMetadata(MetadataOf(body));
        metadata.Should().ContainKey("Socket-Type").WhoseValue.Should().Be("ROUTER");

        var parsed = ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body));
        parsed.Should().NotBeNull();
        parsed.Value.ToArray().Should().Equal(identity);
    }

    [Fact]
    public void ParseReadyIdentity_RejectsDuplicateIdentityProperty()
    {
        var body = BuildReadyWithDuplicateIdentity();

        var act = () => ZmtpCommandCodec.ParseReadyIdentity(MetadataOf(body));
        act.Should().Throw<ZeroMqProtocolException>()
            .WithMessage("*duplicate metadata property 'Identity'*");
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
