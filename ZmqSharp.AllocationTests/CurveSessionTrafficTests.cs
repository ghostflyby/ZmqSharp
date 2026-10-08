using System.Buffers;
using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using ZmqSharp.Security.Curve;
using ZmqSharp.Sockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

/// <summary>Logical frame delivery, authentication and whole-message serialization through the CURVE codec.</summary>
public sealed class CurveSessionTrafficTests
{
    internal static CurveFrameCodec NewCodec() => new(new BouncyCastleCurveCrypto(), Key32.From(new byte[32]),
        new byte[16], new byte[16], 1, 0);

    [Fact]
    public async Task LargeFrame_RoundTrips_WithLongSizeFlag()
    {
        using var raw = new RecordingByteConnection();
        using var session = new ZmtpSession(raw, NewCodec());
        var payload = new byte[300];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)i;
        using var message = ZMessage.FromOwned(payload);
        await session.SendAsync(message);
        var wire = raw.Recorded;
        (wire[0] & (byte)ZmtpFrameFlags.LongSize).Should().NotBe(0);
        var frames = await ReadAsync(wire);
        frames.Should().ContainSingle();
        frames[0].Payload.Should().Equal(payload);
        frames[0].More.Should().BeFalse();
    }

    [Fact]
    public async Task MultiFrameMessage_SendsAllFrames_WithMoreFlags()
    {
        using var raw = new RecordingByteConnection();
        using var session = new ZmtpSession(raw, NewCodec());
        using var message = ZMessage.Copy((byte[][])[[.. "first"u8], [.. "second"u8], [.. "third"u8]]);
        await session.SendAsync(message);
        var frames = await ReadAsync(raw.Recorded);
        frames.Select(frame => frame.More).Should().Equal(true, true, false);
        frames.Select(frame => System.Text.Encoding.ASCII.GetString(frame.Payload)).Should().Equal("first", "second", "third");
    }

    [Fact]
    public async Task ConcurrentMessages_DoNotInterleaveFrames()
    {
        using var raw = new RecordingByteConnection(yieldWrites: true);
        using var session = new ZmtpSession(raw, NewCodec());
        using var one = ZMessage.Copy((byte[][])[[.. "1-a"u8], [.. "1-b"u8], [.. "1-c"u8]]);
        using var two = ZMessage.Copy((byte[][])[[.. "2-a"u8], [.. "2-b"u8], [.. "2-c"u8]]);
        await Task.WhenAll(session.SendAsync(one).AsTask(), session.SendAsync(two).AsTask());
        var frames = await ReadAsync(raw.Recorded);
        frames.Select(frame => frame.More).Should().Equal(true, true, false, true, true, false);
        var groupOne = frames.Take(3).Select(frame => frame.Payload[0]).Distinct().ToArray();
        var groupTwo = frames.Skip(3).Select(frame => frame.Payload[0]).Distinct().ToArray();
        groupOne.Should().ContainSingle();
        groupTwo.Should().ContainSingle();
        groupOne.Should().NotEqual(groupTwo);
    }

    [Fact]
    public async Task TamperedCiphertext_IsRejected()
    {
        var wire = await SealAsync();
        wire[2 + 16 + 5] ^= 0xFF;
        await FluentActions.Awaiting(() => ReadAsync(wire)).Should().ThrowAsync<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReplayedFrame_IsRejected()
    {
        var wire = await SealAsync();
        await FluentActions.Awaiting(() => ReadAsync([.. wire, .. wire])).Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*nonce*");
    }

    [Theory]
    [InlineData(ZmtpFrameFlags.None)]
    [InlineData(ZmtpFrameFlags.More)]
    [InlineData(ZmtpFrameFlags.Command)]
    public void Codec_PreservesFlagsAndEmptyPayload(ZmtpFrameFlags flags)
    {
        using var encoder = NewCodec();
        using var decoder = NewCodec();
        var encoded = encoder.Encode(new ZmtpFrameData { Flags = flags });
        var decoded = decoder.Decode(encoded);
        decoded.Flags.Should().Be(flags);
        decoded.Body.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Codec_RejectsAuthenticatedInvalidFlags()
    {
        using var encoder = NewCodec();
        var encoded = encoder.Encode(new ZmtpFrameData()).Body.ToArray();
        Span<byte> nonce = stackalloc byte[24];
        nonce.Clear();
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], 1);
        new BouncyCastleCurveCrypto().SecretBox([(byte)ZmtpFrameFlags.LongSize], nonce, new byte[32], encoded.AsSpan(16));
        using var decoder = NewCodec();
        FluentActions.Invoking(() => decoder.Decode(new ZmtpFrameData { Body = new(encoded) }))
            .Should().Throw<ZeroMqProtocolException>().WithMessage("*logical flags*");
    }

    [Fact]
    public async Task Parser_ChecksEncodedUpperBoundBeforeReadingBody()
    {
        using var codec = NewCodec();
        codec.GetMaximumEncodedLength(long.MaxValue).Should().Be(long.MaxValue);
        var header = new byte[9];
        header[0] = (byte)ZmtpFrameFlags.LongSize;
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1), codec.GetMaximumEncodedLength(256) + 1);
        using var raw = new RecordingByteConnection(header);
        using var parser = new ZmtpParser(raw, (_, _) => throw new InvalidOperationException("unexpected delivery"),
            null, MemoryPool<byte>.Shared, maxCommandSize: 256, codec: codec, maxFrameLength: 8);
        // There is no body: ignoring the header bound would silently hit EOF.
        await FluentActions.Awaiting(() => parser.ParseAsync().AsTask()).Should().ThrowAsync<ZeroMqProtocolException>()
            .WithMessage("*encoded frame*");
    }

    [Fact]
    public async Task Parser_LogicalLimitRejectsAfterAuthenticationBeforeFinalAllocation()
    {
        using var recording = new RecordingByteConnection();
        using (var session = new ZmtpSession(recording, NewCodec()))
        {
            using var message = ZMessage.FromOwned(new byte[9]);
            await session.SendAsync(message);
        }

        using var input = new RecordingByteConnection(recording.Recorded);
        using var codec = NewCodec();
        using var pool = new CountingRentPool();
        var materializer = new ReceiveMaterializer(pool, new ZReceiveOptions(), 8, 100, 10, () => { });
        using var parser = new ZmtpParser(input, (_, _) => throw new InvalidOperationException("unexpected delivery"),
            materializer.CreateAllocator(), pool, maxCommandSize: 256, codec: codec, maxFrameLength: 8);
        var failure = await FluentActions.Awaiting(() => parser.ParseAsync().AsTask()).Should().ThrowAsync<ZReceiveRejectedException>();
        failure.Which.Rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        pool.Rentals.Should().Be(1, "only bounded ciphertext scratch may be rented");
    }

    [Fact]
    public async Task AwaitedBorrowedCallback_PreservesDecodedBufferUntilConsumed()
    {
        using var recording = new RecordingByteConnection();
        using (var session = new ZmtpSession(recording, NewCodec()))
        {
            using var first = ZMessage.FromOwned([17]);
            using var second = ZMessage.FromOwned([23]);
            await session.SendAsync(first);
            await session.SendAsync(second);
        }

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var raw = new RecordingByteConnection(recording.Recorded);
        using var codec = NewCodec();
        var count = 0;
        using var parser = new ZmtpParser(raw, async (frame, _) =>
        {
            if (++count == 1)
            {
                entered.TrySetResult();
                await release.Task;
                frame[0].Memory.Span[0].Should().Be(17);
            }
            else frame[0].Memory.Span[0].Should().Be(23);

            return true;
        }, codec);
        var parsing = parser.ParseAsync().AsTask();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            count.Should().Be(1);
            parsing.IsCompleted.Should().BeFalse();
        }
        finally
        {
            release.TrySetResult();
        }

        await parsing.WaitAsync(TimeSpan.FromSeconds(5));
        count.Should().Be(2);
    }

    private static async Task<byte[]> SealAsync()
    {
        using var raw = new RecordingByteConnection();
        using var session = new ZmtpSession(raw, NewCodec());
        using var message = ZMessage.FromOwned([.. "secret"u8]);
        await session.SendAsync(message);
        return raw.Recorded;
    }

    private static async Task<List<(bool More, byte[] Payload)>> ReadAsync(byte[] wire)
    {
        using var raw = new RecordingByteConnection(wire);
        using var codec = NewCodec();
        var frames = new List<(bool, byte[])>();
        using var parser = new ZmtpParser(raw, (frame, _) =>
        {
            frames.Add((frame.More, frame.ToSequence().ToArray()));
            return ValueTask.FromResult(true);
        }, codec);
        await parser.ParseAsync();
        return frames;
    }
}
