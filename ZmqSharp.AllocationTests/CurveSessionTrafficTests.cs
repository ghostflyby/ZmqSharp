using System.Buffers.Binary;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Security.Curve;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

/// <summary>
/// Session-level functional coverage of <see cref="CurveSessionConnection"/>
/// that the e2e and allocation tests do not exercise: frames whose payload
/// exceeds the 255-byte long-size boundary, multi-frame message atomicity
/// under concurrent sends, and the negative paths (tampered ciphertext,
/// replayed nonce). These lock the frame reconstruction logic and the
/// whole-message send gate (0023 review fixes).
/// </summary>
public sealed class CurveSessionTrafficTests
{
    private static readonly byte[] Prefix = new byte[16];

    [Fact]
    public async Task LargeFrame_RoundTrips_WithLongSizeFlag()
    {
        var (a, b) = CreatePair();

        var payload = new byte[300];
        for (var i = 0; i < payload.Length; i++) payload[i] = (byte)i;
        await a.SendFrameAsync(payload, false);

        var buffer = new byte[4096];
        var n = await b.ReadAsync(buffer);
        n.Should().BeGreaterThanOrEqualTo(9 + payload.Length);

        // The reconstructed plain frame must carry the LongSize flag, or the
        // traffic parser misreads the 8-byte size as reserved flags.
        (buffer[0] & (byte)ZmtpFrameFlags.LongSize).Should().NotBe(0);
        BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(1, 8)).Should().Be(payload.Length);
        buffer.AsSpan(9, payload.Length).ToArray().Should().Equal(payload);
    }

    [Fact]
    public async Task MultiFrameMessage_SendsAllFrames_WithMoreFlags()
    {
        var (a, b) = CreatePair();

        byte[][] payloads = [[.. "first"u8], [.. "second"u8], [.. "third"u8]];
        await a.SendAsync(Multi(payloads));

        var frames = await ReadFramesAsync(b, 3);
        frames.Should().HaveCount(3);
        for (var i = 0; i < 3; i++) frames[i].Payload.Should().Equal(payloads[i]);
        frames[0].More.Should().BeTrue();
        frames[1].More.Should().BeTrue();
        frames[2].More.Should().BeFalse();
    }

    [Fact]
    public async Task ConcurrentMessages_DoNotInterleaveFrames()
    {
        // The yielding raw write gives a per-frame gate every chance to
        // interleave; the whole-message gate must keep each message's frames
        // contiguous on the wire.
        var (a, b) = CreatePair(yieldWrites: true);

        byte[][] messageOne = [[.. "1-a"u8], [.. "1-b"u8], [.. "1-c"u8]];
        byte[][] messageTwo = [[.. "2-a"u8], [.. "2-b"u8], [.. "2-c"u8]];

        var first = a.SendAsync(Multi(messageOne)).AsTask();
        var second = a.SendAsync(Multi(messageTwo)).AsTask();
        await Task.WhenAll(first, second);

        var frames = await ReadFramesAsync(b, 6);

        // Group the delivered frames into messages: a new group starts right
        // after a final (non-MORE) frame. With the whole-message gate held,
        // each group must be exactly one of the two sent messages - an
        // interleaved write would split them into four groups.
        var grouped = new List<List<byte[]>>();
        var previousWasFinal = true;
        foreach (var frame in frames)
        {
            if (previousWasFinal) grouped.Add([]);
            grouped[^1].Add(frame.Payload);
            previousWasFinal = !frame.More;
        }

        grouped.Should().HaveCount(2);
        grouped.Should().Contain(g => Matches(g, messageOne));
        grouped.Should().Contain(g => Matches(g, messageTwo));
    }

    private static bool Matches(List<byte[]> actual, byte[][] expected)
    {
        if (actual.Count != expected.Length) return false;
        for (var i = 0; i < actual.Count; i++)
            if (!actual[i].AsSpan().SequenceEqual(expected[i]))
                return false;
        return true;
    }

    [Fact]
    public async Task TamperedCiphertext_IsRejected()
    {
        var crypto = new BouncyCastleCurveCrypto();
        var key = Key32.From(new byte[32]);
        var raw = new RecordingConnection();
        using var sealer = new CurveSessionConnection(raw, crypto, key, Prefix, Prefix, 1, 0);

        byte[] payload = [.. "tamper-me"u8];
        await sealer.SendFrameAsync(payload, false);

        // Flip one ciphertext byte (the box starts after the MESSAGE literal
        // and nonce tail, at wire offset header + 16).
        var tampered = raw.Recorded.ToArray();
        tampered[2 + 16 + 5] ^= 0xFF;

        var openerRaw = new RecordingConnection(tampered);
        using var opener = new CurveSessionConnection(openerRaw, crypto, key, Prefix, Prefix, 2, 0);
        var buffer = new byte[4096];
        var act = async () => await opener.ReadAsync(buffer);
        (await Record.ExceptionAsync(act)).Should().BeOfType<ZeroMqProtocolException>();
    }

    [Fact]
    public async Task ReplayedFrame_IsRejected()
    {
        var crypto = new BouncyCastleCurveCrypto();
        var key = Key32.From(new byte[32]);
        var raw = new RecordingConnection();
        using var sealer = new CurveSessionConnection(raw, crypto, key, Prefix, Prefix, 1, 0);

        byte[] payload = [.. "replay-me"u8];
        await sealer.SendFrameAsync(payload, false);

        // Feed the same wire frame twice; the second copy has a nonce tail
        // that no longer increases.
        var wire = raw.Recorded.ToArray();
        var openerRaw = new RecordingConnection([.. wire, .. wire]);
        using var opener = new CurveSessionConnection(openerRaw, crypto, key, Prefix, Prefix, 2, 0);

        var buffer = new byte[4096];
        (await opener.ReadAsync(buffer)).Should().BeGreaterThan(0);
        var act = async () => await opener.ReadAsync(buffer);
        (await Record.ExceptionAsync(act)).Should().BeOfType<ZeroMqProtocolException>();
    }

    // ---- Helpers ----

    private static (CurveSessionConnection, CurveSessionConnection) CreatePair(bool yieldWrites = false)
    {
        var crypto = new BouncyCastleCurveCrypto();
        var key = Key32.From(new byte[32]); // any shared key; the loop is symmetric
        var (aRaw, bRaw) = DuplexConnection.Pair(yieldWrites);
        var a = new CurveSessionConnection(aRaw, crypto, key, Prefix, Prefix, 1, 0);
        var b = new CurveSessionConnection(bRaw, crypto, key, Prefix, Prefix, 1, 0);
        return (a, b);
    }

    private static ZMessage Multi(params byte[][] payloads)
    {
        var frames = new ZFrame[payloads.Length];
        for (var i = 0; i < payloads.Length; i++)
            frames[i] = new ZFrame(new ZSegment(payloads[i], 0, payloads[i].Length));
        return new ZMessage(new ZMultiMessage(frames));
    }

    private static async Task<List<(bool More, byte[] Payload)>> ReadFramesAsync(
        IZConnection connection, int count)
    {
        var frames = new List<(bool More, byte[] Payload)>();
        var buffer = new byte[64 * 1024];
        while (frames.Count < count)
        {
            var n = await connection.ReadAsync(buffer);
            n.Should().BeGreaterThan(0, "the session must deliver the expected frames");

            var offset = 0;
            while (offset < n)
            {
                var flags = buffer[offset];
                var isLong = (flags & 0b0010) != 0;
                var size = isLong
                    ? (int)BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(offset + 1, 8))
                    : buffer[offset + 1];
                offset += isLong ? 9 : 2;
                frames.Add(((flags & 0b0001) != 0, buffer.AsSpan(offset, size).ToArray()));
                offset += size;
            }
        }

        return frames;
    }

    /// <summary>In-memory duplex connection; reads await the peer's writes, so
    /// a full round trip is synchronous.</summary>
    private sealed class DuplexConnection : IZConnection
    {
        private readonly Channel<byte> inbound = Channel.CreateUnbounded<byte>();
        private readonly bool yieldWrites;
        private DuplexConnection? peer;

        private DuplexConnection(bool yieldWrites)
        {
            this.yieldWrites = yieldWrites;
        }

        public static (DuplexConnection, DuplexConnection) Pair(bool yieldWrites)
        {
            var a = new DuplexConnection(yieldWrites);
            var b = new DuplexConnection(yieldWrites);
            a.peer = b;
            b.peer = a;
            return (a, b);
        }

        public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        {
            var filled = 0;
            while (filled < buffer.Length)
            {
                var value = await inbound.Reader.ReadAsync(token);
                buffer.Span[filled] = value;
                filled++;
            }

            return filled;
        }

        public async ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            if (yieldWrites) await Task.Yield();
            foreach (var b in bytes.Span) peer!.inbound.Writer.TryWrite(b);
        }

        public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
        {
            return WriteAsync(frame, token);
        }

        public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        {
            return WriteAsync(body, token);
        }

        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            return ValueTask.CompletedTask;
        }

        public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
        {
        }

        public void SetConnectionEndedHandler(Action onConnectionEnded)
        {
        }

        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
        {
            return ValueTask.FromResult(true);
        }

        public void OnConnectionEnded()
        {
        }

        public void Dispose()
        {
            inbound.Writer.TryComplete();
            peer?.inbound.Writer.TryComplete();
        }
    }

    /// <summary>Connection whose writes are recorded and whose reads serve a
    /// fixed byte stream, both synchronously.</summary>
    private sealed class RecordingConnection : IZConnection
    {
        private readonly List<byte> written = [];
        private readonly byte[] feed;
        private int position;

        public RecordingConnection(byte[]? feed = null)
        {
            this.feed = feed ?? [];
        }

        public byte[] Recorded => written.ToArray();

        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        {
            var count = Math.Min(buffer.Length, feed.Length - position);
            feed.AsSpan(position, count).CopyTo(buffer.Span);
            position += count;
            return ValueTask.FromResult(count);
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            written.AddRange(bytes.Span);
            return ValueTask.CompletedTask;
        }

        public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token = default)
        {
            return WriteAsync(frame, token);
        }

        public ValueTask SendCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
        {
            return WriteAsync(body, token);
        }

        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            return ValueTask.CompletedTask;
        }

        public void SetFrameHandler(Func<ZFrame, CancellationToken, ValueTask<bool>> onFrame)
        {
        }

        public void SetConnectionEndedHandler(Action onConnectionEnded)
        {
        }

        public ValueTask<bool> OnFrameAsync(ZFrame frame, CancellationToken token)
        {
            return ValueTask.FromResult(true);
        }

        public void OnConnectionEnded()
        {
        }

        public void Dispose()
        {
        }
    }
}
