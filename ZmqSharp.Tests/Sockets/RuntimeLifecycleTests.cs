using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

public sealed class RuntimeLifecycleTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disconnect_WaitsForTrackedSendAndCleansBeforePublicEvent(bool throwFromEvent)
    {
        using var pool = new CountingMemoryPool();
        var endpoint = new ByteEndpoint();
        var codec = new IdentityCodec();
        var runtime = new SocketRuntime(new ZSocketOptions
        {
            Security = new ZSecurityOptions { Mechanism = new TestMechanism(codec) }
        }, new ZSinglePeerDispatch(), ZSocketTypes.Pair);
        var observed = false;
        runtime.PeerEnded += (peer, _) =>
        {
            peer.Should().BeSameAs(endpoint.Peer);
            endpoint.Connection.Disposals.Should().Be(1);
            codec.Disposals.Should().Be(1);
            pool.Outstanding.Should().Be(0);
            runtime.PeerSnapshot.Should().BeEmpty();
            observed = true;
            if (throwFromEvent) throw new InvalidOperationException("event failed");
        };
        try
        {
            await runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint);
            endpoint.Peer = runtime.PeerSnapshot.Single();
            await endpoint.Connection.ReadWaiting.Task.WaitAsync(Timeout);
            endpoint.Connection.BlockWrites = true;
            runtime.SendTracked(endpoint.Peer, ZMessage.FromPooled(pool.Rent(8)));
            await endpoint.Connection.WriteStarted.Task.WaitAsync(Timeout);
            var stopping = runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint).AsTask();
            await endpoint.Connection.Aborted.Task.WaitAsync(Timeout);
            stopping.IsCompleted.Should().BeFalse();
            endpoint.Connection.Disposals.Should().Be(0);
            codec.Disposals.Should().Be(0);
            pool.Outstanding.Should().Be(1);
            endpoint.Connection.ReleaseWrite.TrySetResult();
            if (throwFromEvent)
                await FluentActions.Awaiting(() => stopping.WaitAsync(Timeout)).Should().ThrowAsync<InvalidOperationException>()
                    .WithMessage("event failed");
            else await stopping.WaitAsync(Timeout);
            observed.Should().BeTrue();
            await runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint);
        }
        finally
        {
            endpoint.Connection.ReleaseWrite.TrySetResult();
            if (throwFromEvent)
                await FluentActions.Awaiting(async () => await runtime.DisposeAsync()).Should().ThrowAsync<InvalidOperationException>();
            else await runtime.DisposeAsync();
        }
    }

    [Fact]
    public async Task WriteFailure_RetiresPeerAndPreservesFailureForTerminationEvent()
    {
        var endpoint = new ByteEndpoint();
        await using var runtime = new SocketRuntime(new ZSocketOptions(), new ZSinglePeerDispatch(), ZSocketTypes.Pair);
        var ended = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        runtime.PeerEnded += (_, failure) => ended.TrySetResult(failure);
        await runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint);
        endpoint.Connection.BlockWrites = true;
        endpoint.Connection.FailWrite = true;
        var peer = runtime.PeerSnapshot.Single();
        using var borrowed = ZMessage.Copy("write-failure"u8.ToArray());
        var sending = runtime.SendRequestToAsync(peer, borrowed, default).AsTask();
        try
        {
            await endpoint.Connection.WriteStarted.Task.WaitAsync(Timeout);
            endpoint.Connection.ReleaseWrite.TrySetResult();
            await FluentActions.Awaiting(() => sending.WaitAsync(Timeout)).Should().ThrowAsync<IOException>()
                .WithMessage("write failed");
            (await ended.Task.WaitAsync(Timeout)).Should().BeOfType<IOException>();
            runtime.PeerSnapshot.Should().BeEmpty();
            endpoint.Connection.Disposals.Should().Be(1);
        }
        finally { endpoint.Connection.ReleaseWrite.TrySetResult(); }
    }

    [Fact]
    public async Task DisconnectDuringHandshake_WaitsForActualMechanismTaskBeforeReleasingContext()
    {
        using var pool = new CountingMemoryPool();
        var endpoint = new ByteEndpoint();
        var entered = Gate();
        var cancelled = Gate();
        var release = Gate();
        var mechanism = new TestMechanism(null, async (context, token) =>
        {
            await ZNullMechanism.Instance.CreateSession().RunAsync(context, token);
            entered.TrySetResult();
            try { await Task.Delay(System.Threading.Timeout.Infinite, token); }
            catch (OperationCanceledException)
            {
                cancelled.TrySetResult();
                await release.Task;
                throw;
            }
            return null;
        });
        await using var runtime = new SocketRuntime(new ZSocketOptions
        {
            Pool = pool,
            HandshakeTimeoutMs = 0,
            Security = new ZSecurityOptions { Mechanism = mechanism }
        }, new ZSinglePeerDispatch(), ZSocketTypes.Pair);
        var connecting = runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint);
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var stopping = runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint).AsTask();
            await cancelled.Task.WaitAsync(Timeout);
            stopping.IsCompleted.Should().BeFalse();
            endpoint.Connection.Disposals.Should().Be(0);
            pool.Outstanding.Should().Be(1);
            release.TrySetResult();
            await stopping.WaitAsync(Timeout);
            await FluentActions.Awaiting(() => connecting).Should().ThrowAsync<OperationCanceledException>();
            pool.Outstanding.Should().Be(0);
            endpoint.Connection.Disposals.Should().Be(1);
        }
        finally { release.TrySetResult(); }
    }

    private static TaskCompletionSource Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class ByteEndpoint
    {
        public ControlledBytes Connection { get; } = new();
        public ZPeer? Peer { get; set; }
    }

    // Deliberately implements only the public byte contract.
    private sealed class ControlledBytes : IZConnection
    {
        private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"));
        private int position;
        public TaskCompletionSource ReadWaiting { get; } = Gate();
        public TaskCompletionSource WriteStarted { get; } = Gate();
        public TaskCompletionSource ReleaseWrite { get; } = Gate();
        public TaskCompletionSource Aborted { get; } = Gate();
        public int Disposals;
        public bool BlockWrites;
        public bool FailWrite;
        public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        {
            if (position < handshake.Length)
            {
                // Force partial reads throughout the greeting and READY.
                var count = Math.Min(3, Math.Min(destination.Length, handshake.Length - position));
                handshake.AsMemory(position, count).CopyTo(destination);
                position += count;
                return count;
            }
            ReadWaiting.TrySetResult();
            await Aborted.Task;
            return 0;
        }
        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default) => ValueTask.CompletedTask;
        public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
        {
            if (!BlockWrites) return;
            WriteStarted.TrySetResult();
            await ReleaseWrite.Task;
            // Borrowed sequence is still valid after Abort, until the actual write exits.
            bytes.Length.Should().BeGreaterThan(0);
            if (FailWrite) throw new IOException("write failed");
            if (Aborted.Task.IsCompleted) throw new IOException("aborted write");
        }
        public void Abort() => Aborted.TrySetResult();
        public void Dispose() { Abort(); Interlocked.Increment(ref Disposals); }
    }

    private sealed class ByteTransport : IZTransport<ByteTransport, ByteEndpoint>
    {
        public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept { add { } remove { } }
        public static ValueTask<IZConnection> ConnectAsync(ByteEndpoint endpoint, CancellationToken token = default)
            => ValueTask.FromResult<IZConnection>(endpoint.Connection);
        public static ValueTask<ByteTransport> BindAsync(ByteEndpoint endpoint, CancellationToken token = default)
            => throw new NotSupportedException();
        public ValueTask StartAsync(CancellationToken token = default) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class TestMechanism(IdentityCodec? codec,
        Func<ZMechanismContext, CancellationToken, ValueTask<ZMechanismResult?>>? run = null) : IZSecurityMechanism
    {
        public string Name => "NULL";
        public ZMechanismRole Role => ZMechanismRole.None;
        public IZMechanismSession CreateSession() => new Session(codec, run);
        private sealed class Session(IdentityCodec? codec,
            Func<ZMechanismContext, CancellationToken, ValueTask<ZMechanismResult?>>? run) : IZMechanismSession
        {
            public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
            {
                if (run is { } custom) return await custom(context, token);
                var result = await ZNullMechanism.Instance.CreateSession().RunAsync(context, token);
                if (result is { } ready) return new ZMechanismResult(codec, ready.PeerReadyBody);
                return null;
            }
        }
    }

    private sealed class IdentityCodec : IZFrameCodec
    {
        public int Disposals;
        public ZmtpFrameData Encode(ZmtpFrameData frame) => frame;
        public ZmtpFrameData Decode(ZmtpFrameData frame) => frame;
        public long GetMaximumEncodedLength(long length) => length;
        public void Dispose() => Interlocked.Increment(ref Disposals);
    }
}
