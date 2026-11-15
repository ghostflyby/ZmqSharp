using System.Buffers;
using System.Buffers.Binary;
using Xunit;
using ZmqSharp.Zmtp;

namespace ZmqSharp.AllocationTests;

[Collection("allocation-measurement")]
public class CurveTrafficAllocationTests
{
    [Fact]
    public async Task CurveTraffic_SealAndOpen_AreAllocationFreePerFrame()
    {
        const int count = 1000;
        const int warmup = 16;
        using var raw = new RecordingByteConnection(capacity: (count + warmup) * 64);
        using var session = CurveSessionTrafficTests.NewSession(raw, CurveSessionTrafficTests.NewCodec());
        using var message = new ZMessage(new ZSingleMessage(new ZFrame(ZSegment.Borrowed("hello-curve"u8.ToArray()))));
        for (var i = 0; i < warmup; i++) await session.SendAsync(message, TestContext.Current.CancellationToken);
        var thread = Environment.CurrentManagedThreadId;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < count; i++) await session.SendAsync(message, TestContext.Current.CancellationToken);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
#if !DEBUG
        Assert.InRange(allocated, 0, 1023);
#endif
        var wire = raw.Recorded;
        var bodies = new ZmtpFrameData[count + warmup];
        var offset = 0;
        for (var i = 0; i < bodies.Length; i++)
        {
            var longSize = (wire[offset] & 2) != 0;
            var size = longSize ? (int)BinaryPrimitives.ReadInt64BigEndian(wire.AsSpan(offset + 1)) : wire[offset + 1];
            offset += longSize ? 9 : 2;
            bodies[i] = new ZmtpFrameData { Body = new ReadOnlySequence<byte>(wire.AsMemory(offset, size)) };
            offset += size;
        }

        using var codec = CurveSessionTrafficTests.NewCodec();
        for (var i = 0; i < warmup; i++) codec.Decode(bodies[i]);
        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = warmup; i < bodies.Length; i++) codec.Decode(bodies[i]);
        allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
#if !DEBUG
        Assert.InRange(allocated, 0, 1023);
#endif
    }

    [Fact]
    public async Task CurveTraffic_ParserAndDecode_AreAllocationFreePerFrame()
    {
        const int warmup = 16;
        const int count = 1000;
        using var recording = new RecordingByteConnection(capacity: (count + warmup) * 64);
        using (var session = CurveSessionTrafficTests.NewSession(recording, CurveSessionTrafficTests.NewCodec()))
        {
            using var message = ZMessage.FromOwned([.. "parser-curve"u8]);
            for (var i = 0; i < count + warmup; i++) await session.SendAsync(message, TestContext.Current.CancellationToken);
        }

        using var input = new RecordingByteConnection(recording.Recorded);
        using var codec = CurveSessionTrafficTests.NewCodec();
        var seen = 0;
        long before = 0;
        long allocated = 0;
        var thread = Environment.CurrentManagedThreadId;
        using var readSession = ZmqProtocol.Instance.CreateSession(input, codec);
        while (await readSession.ReadMessageAsync(TestContext.Current.CancellationToken) is { } message)
        {
            if (++seen == warmup) before = GC.GetAllocatedBytesForCurrentThread();
            if (seen == count + warmup) allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            message.Dispose();
        }
        Assert.Equal(count + warmup, seen);
        Assert.Equal(thread, Environment.CurrentManagedThreadId);
#if !DEBUG
        Assert.InRange(allocated, 0, 1023);
#endif
    }
}
