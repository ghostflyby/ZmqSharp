using System.Buffers;
using System.Buffers.Binary;
using Xunit;
using ZmqSharp.Security.Curve;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

/// <summary>Logical frame delivery, authentication and whole-message serialization through the CURVE codec.</summary>
public sealed class CurveSessionTrafficTests
{
    internal static CurveFrameCodec NewCodec() => new(Key32.From(new byte[32]),
        encodeServerToClient: false, decodeServerToClient: false, 1, 0);

    internal static IZWireSession NewSession(RecordingByteConnection raw, IZFrameCodec? codec = null)
        => ZmqProtocol.Instance.CreateSession(ZmtpFraming.Instance.Adapt(raw), codec);

    [Fact]
    public async Task LargeFrame_RoundTrips_WithLongSizeFlag()
    {
        var token = TestContext.Current.CancellationToken;
        using var raw = new RecordingByteConnection();
        using var session = NewSession(raw, NewCodec());
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
        using var session = NewSession(raw, NewCodec());
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
        using var session = NewSession(raw, NewCodec());
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
        CurveCrypto.SecretBox([(byte)ZmtpFrameFlags.LongSize], nonce, new byte[32], encoded.AsSpan(16));
        using var decoder = NewCodec();
        var exception = Assert.Throws<ZeroMqProtocolException>(() => decoder.Decode(new ZmtpFrameData { Body = new ReadOnlySequence<byte>(encoded) }));
        Assert.Contains("logical flags", exception.Message);
    }

    [Fact]
    public async Task Framing_ChecksEncodedUpperBoundBeforeReadingBody()
    {
        var token = TestContext.Current.CancellationToken;
        using var codec = NewCodec();
        Assert.Equal(long.MaxValue, codec.GetMaximumEncodedLength(long.MaxValue));
        var header = new byte[9];
        header[0] = (byte)ZmtpFrameFlags.LongSize;
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(1), codec.GetMaximumEncodedLength(256) + 1);
        using var raw = new RecordingByteConnection(header);
        // The codec-aware bound rides on the framing: a header declaring an
        // encoded size beyond it rejects before any body is read.
        using var session = ZmqProtocol.Instance.CreateSession(
            ZmtpFraming.Instance.Adapt(raw, null, 256), null);
        // There is no body: ignoring the header bound would silently hit EOF.
        var exception = await Assert.ThrowsAsync<ZeroMqProtocolException>(
            () => session.ReadMessageAsync(token).AsTask());
        Assert.Contains("exceeds maximum length", exception.Message);
    }

    [Fact]
    public async Task Pump_LogicalLimitRejectsAfterAuthenticationBeforeFinalAllocation()
    {
        var token = TestContext.Current.CancellationToken;
        using var recording = new RecordingByteConnection();
        using (var session = NewSession(recording, NewCodec()))
        {
            using var message = ZMessage.FromOwned(new byte[9]);
            await session.SendAsync(message, token);
        }

        using var input = new RecordingByteConnection(recording.Recorded);
        using var codec = NewCodec();
        using var pool = new CountingRentPool();
        var materializer = new ReceiveMaterializer(pool, new ZReceiveOptions(), 8, 100, 10, () => { });
        // The framing's pooled rent carries the encoded body (the old parser's
        // scratch rent); the decoded frame then materializes through the
        // allocator, whose 8-byte limit rejects before renting.
        using var readSession = ZmqProtocol.Instance.CreateSession(
            ZmtpFraming.Instance.Adapt(input, null, long.MaxValue, pool), codec,
            decodedAllocator: materializer.CreateAllocator(), maxCommandSize: 256, maxFrameLength: 8);
        var failure = await Assert.ThrowsAsync<ZReceiveRejectedException>(
            () => readSession.ReadMessageAsync(token).AsTask());
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, failure.Rejection.Reason);
        // The framing scratch plus one recycled owner for the encoded
        // aggregate (0033); the rejected frame never rents.
        Assert.Equal(2, pool.Rentals);
    }

    [Fact(Timeout = 15_000)]
    public async Task AwaitedBorrowedCallback_PreservesDecodedBufferUntilConsumed()
    {
        var token = TestContext.Current.CancellationToken;
        using var recording = new RecordingByteConnection();
        using (var sealSession = NewSession(recording, NewCodec()))
        {
            using var first = ZMessage.FromOwned([17]);
            using var second = ZMessage.FromOwned([23]);
            await sealSession.SendAsync(first, token);
            await sealSession.SendAsync(second, token);
        }

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var raw = new RecordingByteConnection(recording.Recorded);
        using var codec = NewCodec();
        using var session = ZmqProtocol.Instance.CreateSession(ZmtpFraming.Instance.Adapt(raw), codec);
        var count = 0;
        var parsing = Task.Run(async () =>
        {
            while (await session.ReadMessageAsync(token) is { } message)
            {
                using (message)
                {
                    if (++count == 1)
                    {
                        entered.TrySetResult();
                        await release.Task;
                        Assert.Equal(17, message[0][0].Memory.Span[0]);
                    }
                    else Assert.Equal(23, message[0][0].Memory.Span[0]);
                }
            }
        }, token);
        try
        {
            await entered.Task.WaitAsync(token);
            Assert.Equal(1, count);
            Assert.False(parsing.IsCompleted);
        }
        finally
        {
            release.SetResult();
        }

        await parsing.WaitAsync(token);
        Assert.Equal(2, count);
    }

    private static async Task<byte[]> SealAsync()
    {
        using var raw = new RecordingByteConnection();
        using var session = NewSession(raw, NewCodec());
        using var message = ZMessage.FromOwned([.. "secret"u8]);
        await session.SendAsync(message);
        return raw.Recorded;
    }

    private static async Task<List<(bool More, byte[] Payload)>> ReadAsync(byte[] wire)
    {
        using var raw = new RecordingByteConnection(wire);
        using var codec = NewCodec();
        var frames = new List<(bool, byte[])>();
        using var session = ZmqProtocol.Instance.CreateSession(ZmtpFraming.Instance.Adapt(raw), codec);
        while (await session.ReadMessageAsync() is { } message)
        {
            foreach (var frame in message) frames.Add((frame.More, frame.ToSequence().ToArray()));
            message.Dispose();
        }
        return frames;
    }
}
