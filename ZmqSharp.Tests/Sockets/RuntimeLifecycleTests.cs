using System.Buffers;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Security;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Sockets;

public sealed class RuntimeLifecycleTests
{
    [Theory(Timeout = 20_000)]
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
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        runtime.PeerEnded += (peer, _) =>
        {
            Assert.Same(endpoint.Peer, peer);
            Assert.Equal(1, endpoint.Connection.Disposals);
            Assert.Equal(1, codec.Disposals);
            // ReSharper disable once AccessToDisposedClosure
            Assert.Equal(0, pool.Outstanding);
            Assert.Empty(runtime.PeerSnapshot);
            observed = true;
            if (throwFromEvent) throw new InvalidOperationException("event failed");
        };
        var token = TestContext.Current.CancellationToken;
        try
        {
            await runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint, token);
            endpoint.Peer = runtime.PeerSnapshot.Single();
            await endpoint.Connection.ReadWaiting.Task.WaitAsync(token);
            endpoint.Connection.BlockWrites = true;
            runtime.SendTracked(endpoint.Peer, ZMessage.FromPooled(pool.Rent(8)));
            await endpoint.Connection.WriteStarted.Task.WaitAsync(token);
            var stopping = runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint, token).AsTask();
            await endpoint.Connection.Aborted.Task.WaitAsync(token);
            Assert.False(stopping.IsCompleted);
            Assert.Equal(0, endpoint.Connection.Disposals);
            Assert.Equal(0, codec.Disposals);
            Assert.Equal(1, pool.Outstanding);
            endpoint.Connection.ReleaseWrite.TrySetResult();
            if (throwFromEvent)
            {
                var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => stopping.WaitAsync(token));
                Assert.Equal("event failed", failure.Message);
            }
            else await stopping.WaitAsync(token);
            Assert.True(observed);
            await runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint, token);
        }
        finally
        {
            endpoint.Connection.ReleaseWrite.TrySetResult();
            if (throwFromEvent)
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await runtime.DisposeAsync());
            else await runtime.DisposeAsync();
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task WriteFailure_RetiresPeerAndPreservesFailureForTerminationEvent()
    {
        var endpoint = new ByteEndpoint();
        await using var runtime = new SocketRuntime(new ZSocketOptions(), new ZSinglePeerDispatch(), ZSocketTypes.Pair);
        var ended = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        runtime.PeerEnded += (_, failure) => ended.TrySetResult(failure);
        var token = TestContext.Current.CancellationToken;
        await runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint, token);
        endpoint.Connection.BlockWrites = true;
        endpoint.Connection.FailWrite = true;
        var peer = runtime.PeerSnapshot.Single();
        using var borrowed = ZMessage.Copy("write-failure"u8.ToArray());
        var sending = runtime.SendRequestToAsync(peer, borrowed, token).AsTask();
        try
        {
            await endpoint.Connection.WriteStarted.Task.WaitAsync(token);
            endpoint.Connection.ReleaseWrite.TrySetResult();
            var failure = await Assert.ThrowsAsync<IOException>(() => sending.WaitAsync(token));
            Assert.Equal("write failed", failure.Message);
            Assert.IsType<IOException>(await ended.Task.WaitAsync(token));
            Assert.Empty(runtime.PeerSnapshot);
            Assert.Equal(1, endpoint.Connection.Disposals);
        }
        finally
        {
            endpoint.Connection.ReleaseWrite.TrySetResult();
        }
    }

    [Fact]
    public async Task RejectedPeerType_WithFailedErrorWrite_SurfacesTheFailureNotCancellation()
    {
        // The peer advertises a socket type the local type rejects, and the peer
        // has already closed, so the local ERROR write faults. The awaiting
        // connect must report that write failure; the peer's side of the race
        // ends establishment, but nobody canceled the connect (0030 section 6).
        var endpoint = new ClosingEndpoint();
        await using var runtime = new SocketRuntime(new ZSocketOptions(), new ZSinglePeerDispatch(),
            ZSocketType.ForCustom("FOO"));
        var token = TestContext.Current.CancellationToken;

        var failure = await Assert.ThrowsAsync<IOException>(() => runtime.ConnectAsync<ClosingEndpoint, ClosingTransport>(endpoint, token));
        Assert.Equal("peer closed before the ERROR write", failure.Message);
        Assert.Empty(runtime.PeerSnapshot);
    }

    [Fact(Timeout = 15_000)]
    public async Task DisconnectDuringHandshake_WaitsForActualMechanismTaskBeforeReleasingContext()
    {
        using var pool = new CountingMemoryPool();
        var endpoint = new ByteEndpoint();
        var entered = Gate();
        var cancelled = Gate();
        var release = Gate();
        var token = TestContext.Current.CancellationToken;
        var mechanism = new TestMechanism(null, async (context, mechanismToken) =>
        {
            await ZNullMechanism.Instance.CreateSession().RunAsync(context, mechanismToken);
            entered.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, mechanismToken);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
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
        var connecting = runtime.ConnectAsync<ByteEndpoint, ByteTransport>(endpoint, token);
        try
        {
            await entered.Task.WaitAsync(token);
            var stopping = runtime.DisconnectAsync<ByteEndpoint, ByteTransport>(endpoint, token).AsTask();
            await cancelled.Task.WaitAsync(token);
            Assert.False(stopping.IsCompleted);
            Assert.Equal(0, endpoint.Connection.Disposals);
            Assert.Equal(1, pool.Outstanding);
            release.TrySetResult();
            await stopping.WaitAsync(token);
            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connecting);
            Assert.False(token.IsCancellationRequested,
                $"token.IsCancellationRequested was {token.IsCancellationRequested} but expected the connect to fail independently of the test token");
            Assert.Equal(0, pool.Outstanding);
            Assert.Equal(1, endpoint.Connection.Disposals);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    private static TaskCompletionSource Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class ClosingEndpoint
    {
        public ClosingBytes Connection { get; } = new();
    }

    /// <summary>
    /// Serves a greeting and a READY advertising PAIR, then fails the second
    /// sequence write: the mechanism's READY succeeds, and the ERROR command
    /// that follows the local socket-type rejection finds the peer gone.
    /// </summary>
    private sealed class ClosingBytes : IZConnection
    {
        private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready());
        private int position;
        private int sequenceWrites;
        private TaskCompletionSource Aborted { get; } = Gate();
        private int disposals;

        public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
        {
            if (position < handshake.Length)
            {
                var count = Math.Min(destination.Length, handshake.Length - position);
                handshake.AsMemory(position, count).CopyTo(destination);
                position += count;
                return count;
            }

            await Aborted.Task;
            return 0;
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
            => ValueTask.CompletedTask;

        public ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
            => Interlocked.Increment(ref sequenceWrites) > 1
                ? throw new IOException("peer closed before the ERROR write")
                : ValueTask.CompletedTask;

        public void Abort() => Aborted.TrySetResult();

        public void Dispose()
        {
            Abort();
            Interlocked.Increment(ref disposals);
        }
    }

    // Connect-only phantom: the type carries its static ConnectAsync factory
    // and satisfies the IZTransport CRTP constraint; BindAsync throws, so no
    // instance can ever exist.
    // ReSharper disable once ClassNeverInstantiated.Local
    private sealed class ClosingTransport : IZTransport<ClosingTransport, ClosingEndpoint>
    {
        public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
        {
            add { }
            remove { }
        }

        public static ValueTask<IZConnection> ConnectAsync(ClosingEndpoint endpoint, CancellationToken token = default)
            => ValueTask.FromResult<IZConnection>(endpoint.Connection);

        public static ValueTask<ClosingTransport> BindAsync(ClosingEndpoint endpoint, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask StartAsync(CancellationToken token = default) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class ByteEndpoint
    {
        public ControlledBytes Connection { get; } = new();
        public ZPeer? Peer { get; set; }
    }

    // Deliberately implements only the public byte contract.
    private sealed class ControlledBytes : IZConnection
    {
        private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready());
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
            Assert.True(bytes.Length > 0, $"bytes.Length was {bytes.Length} but expected greater than 0");
            if (FailWrite) throw new IOException("write failed");
            if (Aborted.Task.IsCompleted) throw new IOException("aborted write");
        }

        public void Abort() => Aborted.TrySetResult();

        public void Dispose()
        {
            Abort();
            Interlocked.Increment(ref Disposals);
        }
    }

    // Connect-only phantom: the type carries its static ConnectAsync factory
    // and satisfies the IZTransport CRTP constraint; BindAsync throws, so no
    // instance can ever exist.
    // ReSharper disable once ClassNeverInstantiated.Local
    private sealed class ByteTransport : IZTransport<ByteTransport, ByteEndpoint>
    {
        public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
        {
            add { }
            remove { }
        }

        public static ValueTask<IZConnection> ConnectAsync(ByteEndpoint endpoint, CancellationToken token = default)
            => ValueTask.FromResult<IZConnection>(endpoint.Connection);

        public static ValueTask<ByteTransport> BindAsync(ByteEndpoint endpoint, CancellationToken token = default)
            => throw new NotSupportedException();

        public ValueTask StartAsync(CancellationToken token = default) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class TestMechanism(
        IdentityCodec? codec,
        Func<ZMechanismContext, CancellationToken, ValueTask<ZMechanismResult?>>? run = null) : IZSecurityMechanism
    {
        public string Name => "NULL";
        public ZMechanismRole Role => ZMechanismRole.None;
        public IZMechanismSession CreateSession() => new Session(codec, run);

        private sealed class Session(
            IdentityCodec? codec,
            Func<ZMechanismContext, CancellationToken, ValueTask<ZMechanismResult?>>? run) : IZMechanismSession
        {
            public async ValueTask<ZMechanismResult?> RunAsync(ZMechanismContext context, CancellationToken token)
            {
                if (run is not null) return await run(context, token);
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
