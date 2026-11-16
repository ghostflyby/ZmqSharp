using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Zmtp;

/// <summary>
/// Send-side ownership through the codec path (0034): the session encodes a
/// new message, so the caller's input is never transferred; the assembled
/// message belongs to the session unless the connection retains it.
/// </summary>
public sealed class ProtocolSessionOwnershipTests
{
    [Fact(Timeout = 10_000)]
    public async Task CopyingConnection_CodecSend_ReleasesTheAssembledMessage()
    {
        using var pool = new CountingMemoryPool();
        var connection = new RecordingConnection(pool) { Retains = false };
        using var session = ZmqProtocol.Instance.CreateSession(connection, new PassThroughCodec(), pool: pool);
        using var message = MessageFactory.Multipart([1], [2], [3]);

        await session.SendAsync(message, TestContext.Current.CancellationToken);

        Assert.Equal(3, connection.ObservedOutstanding); // one staged owner per frame
        Assert.Equal(0, pool.Outstanding); // the session released them after the write
        Assert.False(session.RetainsSentMessages); // the input never reached the connection
    }

    [Fact(Timeout = 10_000)]
    public async Task RetainingConnection_CodecSend_TransfersOnlyTheAssembledMessage()
    {
        using var pool = new CountingMemoryPool();
        var connection = new RecordingConnection(pool) { Retains = true };
        using var session = ZmqProtocol.Instance.CreateSession(connection, new PassThroughCodec(), pool: pool);
        using var message = MessageFactory.Multipart([1], [2], [3]);

        await session.SendAsync(message, TestContext.Current.CancellationToken);

        Assert.Equal(3, pool.Outstanding); // ownership transferred to the connection
        Assert.False(session.RetainsSentMessages); // the caller's input is not retained
        connection.Dispose();
        Assert.Equal(0, pool.Outstanding);
    }

    private sealed class PassThroughCodec : IZFrameCodec
    {
        public ZmtpFrameData Encode(ZmtpFrameData logicalFrame) => logicalFrame;

        public ZmtpFrameData Decode(ZmtpFrameData wireFrame) => wireFrame;

        public long GetMaximumEncodedLength(long logicalBodyLimit) => logicalBodyLimit;

        public void Dispose()
        {
        }
    }

    private sealed class RecordingConnection(CountingMemoryPool pool) : IZMessageConnection
    {
        private ZMessage _held;

        public bool Retains { get; init; }

        public int ObservedOutstanding { get; private set; }

        public bool RetainsMessages => Retains;

        public ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
            => ValueTask.FromResult<ZWireUnit?>(null);

        public ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
        {
            ObservedOutstanding = pool.Outstanding;
            if (Retains) _held = message;
            return ValueTask.CompletedTask;
        }

        public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
            => ValueTask.CompletedTask;

        public void Abort()
        {
        }

        public void Dispose() => _held.Dispose();
    }
}
