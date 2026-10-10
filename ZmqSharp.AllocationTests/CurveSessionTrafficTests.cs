using System.Buffers;
using System.Buffers.Binary;
using Xunit;
using ZmqSharp.Security.Curve;
using ZmqSharp.Sockets;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

/// <summary>Logical frame delivery, authentication and whole-message serialization through the CURVE codec.</summary>
public sealed class CurveSessionTrafficTests
{
    internal static CurveFrameCodec NewCodec() => new(new BouncyCastleCurveCrypto(), Key32.From(new byte[32]),
        encodeServerToClient: false, decodeServerToClient: false, 1, 0);

    [Fact]
    public async Task LargeFrame_RoundTrips_WithLongSizeFlag()
    {
        var token = TestContext.Current.CancellationToken;
        using var raw = new RecordingByteConnection();
        using var session = new ZmtpSession(raw, NewCodec());
        var payload = new byte[300];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)i;
        using var message = ZMessage.FromOwned(payload);
        await session.SendAsync(message, token);
        var wire = raw.Recorded;
        Assert.NotEqual(0, wire[0] & (byte)ZmtpFrameFlags.LongSize);
        var frames = await ReadAsync(wire);
        var frame = Assert.Single(frames);
        Assert.Equal(payload, frame.Payload);
        Assert.False(frame.More);
    }

    [Fact]
    public async Task MultiFrameMessage_SendsAllFrames_WithMoreFlags()
    {
        var token = TestContext.Current.CancellationToken;
        using var raw = new RecordingByteConnection();
        using var session = new ZmtpSession(raw, NewCodec());
        using var message = ZMessage.Copy((byte[][])[[.. "first"u8], [.. "second"u8], [.. "third"u8]]);
        await session.SendAsync(message, token);
        var frames = await ReadAsync(raw.Recorded);
        Assert.Equal([true, true, false], frames.Select(frame => frame.More).ToArray());
        Assert.Equal(["first", "second", "third"],
            frames.Select(frame => System.Text.Encoding.ASCII.GetString(frame.Payload)).ToArray());
    }

    [Fact]
    public async Task ConcurrentMessages_DoNotInterleaveFrames()
    {
        var token = TestContext.Current.CancellationToken;
        using var raw = new RecordingByteConnection(yieldWrites: true);
        using var session = new ZmtpSession(raw, NewCodec());
        using var one = ZMessage.Copy((byte[][])[[.. "1-a"u8], [.. "1-b"u8], [.. "1-c"u8]]);
        using var two = ZMessage.Copy((byte[][])[[.. "2-a"u8], [.. "2-b"u8], [.. "2-c"u8]]);
        await Task.WhenAll(session.SendAsync(one, token).AsTask(), session.SendAsync(two, token).AsTask());
        var frames = await ReadAsync(raw.Recorded);
        Assert.Equal([true, true, false, true, true, false], frames.Select(frame => frame.More).ToArray());
        var groupOne = frames.Take(3).Select(frame => frame.Payload[0]).Distinct().ToArray();
        var groupTwo = frames.Skip(3).Select(frame => frame.Payload[0]).Distinct().ToArray();
        Assert.Single(groupOne);
        Assert.Single(groupTwo);
        Assert.NotEqual(groupTwo, groupOne);
    }

    [Fact]
    public async Task TamperedCiphertext_IsRejected()
    {
        var wire = await SealAsync();
        wire[2 + 16 + 5] ^= 0xFF;
        await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ReadAsync(wire));
    }

    [Fact]
    public async Task ReplayedFrame_IsRejected()
    {
        var wire = await SealAsync();
        var exception = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => ReadAsync([.. wire, .. wire]));
        Assert.Contains("nonce", exception.Message);
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
        Assert.Equal(flags, decoded.Flags);
        Assert.True(decoded.Body.IsEmpty);
    }

    [Fact]
    public void Codec_RejectsAuthenticatedInvalidFlags()
    {
        using var encoder = NewCodec();
        var encoded = encoder.Encode(new ZmtpFrameData()).Body.ToArray();
        Span<byte> nonce = stackalloc byte[24];
        CurveConstants.MessagePrefixClientToServer.CopyTo(nonce);
        BinaryPrimitives.WriteUInt64BigEndian(nonce[16..], 1);
        new BouncyCastleCurveCrypto().SecretBox([(byte)ZmtpFrameFlags.LongSize], nonce, new byte[32], encoded.AsSpan(16));
        using var decoder = NewCodec();
        var exception = Assert.Throws<ZeroMqProtocolException>(() => decoder.Decode(new ZmtpFrameData { Body = new ReadOnlySequence<byte>(encoded) }));
        Assert.Contains("logical flags", exception.Message);
    }

    [Fact]
    public async Task Parser_ChecksEncodedUpperBoundBeforeReadingBody()
    {
        var token = TestContext.Current.CancellationToken;
        using var codec = NewCodec();
        Assert.Equal(long.MaxValue, codec.GetMaximumEncodedLength(long.MaxValue));
        var header = new byte[9];
        header[0] = (byte)ZmtpFrameFlags.LongSize;
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1), codec.GetMaximumEncodedLength(256) + 1);
        using var raw = new RecordingByteConnection(header);
        using var parser = new ZmtpParser(raw, (_, _) => throw new InvalidOperationException("unexpected delivery"),
            null, MemoryPool<byte>.Shared, maxCommandSize: 256, codec: codec, maxFrameLength: 8);
        // There is no body: ignoring the header bound would silently hit EOF.
        var exception = await Assert.ThrowsAsync<ZeroMqProtocolException>(() => parser.ParseAsync(token).AsTask());
        Assert.Contains("encoded frame", exception.Message);
    }

    [Fact]
    public async Task Parser_LogicalLimitRejectsAfterAuthenticationBeforeFinalAllocation()
    {
        var token = TestContext.Current.CancellationToken;
        using var recording = new RecordingByteConnection();
        using (var session = new ZmtpSession(recording, NewCodec()))
        {
            using var message = ZMessage.FromOwned(new byte[9]);
            await session.SendAsync(message, token);
        }

        using var input = new RecordingByteConnection(recording.Recorded);
        using var codec = NewCodec();
        using var pool = new CountingRentPool();
        var materializer = new ReceiveMaterializer(pool, new ZReceiveOptions(), 8, 100, 10, () => { });
        using var parser = new ZmtpParser(input, (_, _) => throw new InvalidOperationException("unexpected delivery"),
            materializer.CreateAllocator(), pool, maxCommandSize: 256, codec: codec, maxFrameLength: 8);
        var failure = await Assert.ThrowsAsync<ZReceiveRejectedException>(() => parser.ParseAsync(token).AsTask());
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, failure.Rejection.Reason);
        Assert.Equal(1, pool.Rentals);
    }

    [Fact(Timeout = 15_000)]
    public async Task AwaitedBorrowedCallback_PreservesDecodedBufferUntilConsumed()
    {
        var token = TestContext.Current.CancellationToken;
        using var recording = new RecordingByteConnection();
        using (var session = new ZmtpSession(recording, NewCodec()))
        {
            using var first = ZMessage.FromOwned([17]);
            using var second = ZMessage.FromOwned([23]);
            await session.SendAsync(first, token);
            await session.SendAsync(second, token);
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
                Assert.Equal(17, frame[0].Memory.Span[0]);
            }
            else Assert.Equal(23, frame[0].Memory.Span[0]);

            return true;
        }, codec);
        var parsing = parser.ParseAsync(token).AsTask();
        try
        {
            await entered.Task.WaitAsync(token);
            Assert.Equal(1, count);
            Assert.False(parsing.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
        }

        await parsing.WaitAsync(token);
        Assert.Equal(2, count);
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
