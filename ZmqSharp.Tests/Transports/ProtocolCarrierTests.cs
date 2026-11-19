using System.Buffers;
using System.Net;
using Xunit;
using ZmqSharp.Security;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// A connection that carries its own protocol wins over the socket default
/// (0034/0036); an explicit <see cref="ZSocketOptions.WireProtocol"/> still
/// wins over the carrier.
/// </summary>
public sealed class ProtocolCarrierTests
{
    [Fact(Timeout = 15_000)]
    public async Task ConnectionCarriedProtocol_IsUsedForEstablishment()
    {
        CarrierTransport.Protocol.Reset();
        await using var socket = new ZPairSocket();
        var token = TestContext.Current.CancellationToken;

        var error = await Assert.ThrowsAsync<IOException>(
            () => socket.ConnectAsync<EndPoint, CarrierTransport>(
                new IPEndPoint(IPAddress.Loopback, 0), token));

        Assert.Contains("peer closed during handshake", error.Message);
        Assert.Equal(1, CarrierTransport.Protocol.Establishments);
    }

    [Fact(Timeout = 15_000)]
    public async Task ExplicitWireProtocol_WinsOverTheConnectionCarrier()
    {
        var explicitProtocol = new RecordingProtocol();
        await using var socket = new ZPairSocket(new ZSocketOptions { WireProtocol = explicitProtocol });
        var token = TestContext.Current.CancellationToken;

        var error = await Assert.ThrowsAsync<IOException>(
            () => socket.ConnectAsync<EndPoint, CarrierTransport>(
                new IPEndPoint(IPAddress.Loopback, 0), token));

        Assert.Contains("peer closed during handshake", error.Message);
        Assert.Equal(1, explicitProtocol.Establishments);
    }

    private sealed class CarrierTransport : IZClientTransport<CarrierTransport, EndPoint>
    {
        public static RecordingProtocol Protocol { get; } = new();

        public static ValueTask<IZMessageConnection> ConnectAsync(
            EndPoint endpoint, ZTransportContext context, CancellationToken token = default)
            => ValueTask.FromResult<IZMessageConnection>(new CarrierConnection());
    }

    private sealed class CarrierConnection : IZMessageConnection, IZProtocolCarrier
    {
        public IZWireProtocol Protocol => CarrierTransport.Protocol;

        public bool RetainsMessages => false;

        public ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
            => ValueTask.FromResult<ZWireUnit?>(null);

        public ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
            => ValueTask.CompletedTask;

        public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default)
            => ValueTask.CompletedTask;

        public void Abort() { }

        public void Dispose() { }
    }

    private sealed class RecordingProtocol : IZWireProtocol
    {
        private int _establishments;

        public int Establishments => Volatile.Read(ref _establishments);

        public void Reset() => Interlocked.Exchange(ref _establishments, 0);

        public ValueTask<ZMechanismResult?> EstablishAsync(
            IZMessageConnection connection, IZSecurityMechanism mechanism,
            ReadOnlyMemory<byte> localReadyBody, long maxCommandSize,
            CancellationToken token = default)
        {
            Interlocked.Increment(ref _establishments);
            return ValueTask.FromResult<ZMechanismResult?>(null);
        }

        public IZWireSession CreateSession(IZMessageConnection connection, IZFrameCodec? codec,
            Action<Exception>? retire = null, ZFrameAllocator? decodedAllocator = null,
            long maxCommandSize = ZmtpFraming.DefaultMaxCommandSize, long maxFrameLength = long.MaxValue,
            MemoryPool<byte>? pool = null)
            => throw new NotSupportedException();
    }
}
