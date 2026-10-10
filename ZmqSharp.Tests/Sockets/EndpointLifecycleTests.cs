using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

public sealed class EndpointLifecycleTests
{
    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stop_WaitsForCleanupAndRepeatedCallsShareCompletion(bool listener)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = new ControlledEndpoint();
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        if (listener) await socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        else await socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        await endpoint.Parked.Task.WaitAsync(token);
        if (listener) socket.Unbind<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        else socket.Disconnect<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.Stopping.Task.WaitAsync(token);
        var first = listener
            ? socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token).AsTask()
            : socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token).AsTask();
        var second = listener
            ? socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token).AsTask()
            : socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token).AsTask();
        first.IsCompleted.Should().BeFalse();
        second.IsCompleted.Should().BeFalse();
        endpoint.Release.TrySetResult();
        await Task.WhenAll(first, second).WaitAsync(token);
        endpoint.Disposals.Should().Be(1);
        socket.PeerSnapshot.Should().BeEmpty();
    }

    [Theory(Timeout = 20_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationOnlyCancelsWait_AndTransportTypeIsPartOfIdentity(bool listener)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = new ControlledEndpoint();
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        if (listener) await socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        else await socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        await endpoint.Parked.Task.WaitAsync(token);
        if (listener) await socket.UnbindAsync<ControlledEndpoint, ControlledTransport<Second>>(endpoint, token);
        else await socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<Second>>(endpoint, token);
        endpoint.Stopping.Task.IsCompleted.Should().BeFalse();

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await FluentActions.Awaiting(async () =>
        {
            if (listener) await socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, cancellation.Token);
            else await socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, cancellation.Token);
        }).Should().ThrowAsync<OperationCanceledException>();
        endpoint.Stopping.Task.IsCompleted.Should().BeFalse();

        using var waitCancellation = new CancellationTokenSource();
        var waiting = listener
            ? socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, waitCancellation.Token).AsTask()
            : socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, waitCancellation.Token).AsTask();
        await endpoint.Stopping.Task.WaitAsync(token);
        await waitCancellation.CancelAsync();
        await FluentActions.Awaiting(() => waiting).Should().ThrowAsync<OperationCanceledException>();
        endpoint.Release.TrySetResult();
        if (listener) await socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        else await socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        endpoint.Disposals.Should().Be(1);
    }

    [Fact(Timeout = 30_000)]
    public async Task DisconnectDuringHandshake_WaitsForItsCleanup()
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = new ControlledEndpoint { Handshake = false };
        await using var socket = new ZPairSocket(new ZSocketOptions { HandshakeTimeoutMs = 0 });
        using var cleanup = new ReleaseOnDispose(endpoint);
        var connecting = socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        await endpoint.Parked.Task.WaitAsync(token);
        var disconnecting = socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token).AsTask();
        await endpoint.Stopping.Task.WaitAsync(token);
        disconnecting.IsCompleted.Should().BeFalse();
        endpoint.Release.TrySetResult();
        await disconnecting.WaitAsync(token);
        await FluentActions.Awaiting(() => connecting).Should().ThrowAsync<OperationCanceledException>()
            .Where(failure => !token.IsCancellationRequested);
    }

    [Fact]
    public async Task UnknownStringEndpoint_DoesNotResolveDns()
    {
        var token = TestContext.Current.CancellationToken;
        await using IZSocket socket = new ZPairSocket();
        await socket.UnbindAsync("tcp://does-not-exist.invalid:12345", token);
        await socket.DisconnectAsync("tcp://does-not-exist.invalid:12345", token);
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task StringEndpoints_UnbindPreservesAcceptedPeer_AndDisconnectWaitsForEnded(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var address = TestTransports.GetEndpoint(kind);
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        await server.BindAsync(address, token);
        await client.ConnectAsync(address, token);
        await server.UnbindAsync(address, token);
        await client.SendAsync("still connected"u8.ToArray(), token);
        using var message = await server.Messages.ReadAsync(token);
        message[0].ToSequence().ToArray().Should().Equal("still connected"u8.ToArray());
        var ended = false;
        client.PeerEnded += (_, _) => ended = true;
        await client.DisconnectAsync(address, token);
        ended.Should().BeTrue();
        client.PeerSnapshot.Should().BeEmpty();
        await client.DisconnectAsync(address, token);
        if (kind == TransportKind.Ipc)
        {
            await server.BindAsync(address, token);
            await server.UnbindAsync(address, token);
        }
    }

    [Theory(Timeout = 20_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DisconnectWithFullReceiveQueue_CancelsBackpressureAndReclaimsBuffers(TransportKind kind)
    {
        var token = TestContext.Current.CancellationToken;
        var address = TestTransports.GetEndpoint(kind);
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket();
        await using var receiver = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait }
        });
        await server.BindAsync(address, token);
        await receiver.ConnectAsync(address, token);
        var baseline = pool.Outstanding;
        await server.SendAsync("first"u8.ToArray(), token);
        await server.SendAsync("second"u8.ToArray(), token);
        await pool.WaitForOutstandingAtLeastAsync(baseline + 2, TimeSpan.FromSeconds(10));
        await receiver.DisconnectAsync(address, token);
        pool.Outstanding.Should().Be(0);
        receiver.Messages.TryRead(out _).Should().BeFalse();
    }

    [Theory(Timeout = 20_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FactoryReturnsAfterSocketCloses_ResourceIsReleasedWithoutStartingPump(bool listening)
    {
        var token = TestContext.Current.CancellationToken;
        var endpoint = new ControlledEndpoint { DelayFactory = true };
        var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        var setup = listening
            ? socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token)
            : socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, token);
        await endpoint.FactoryEntered.Task.WaitAsync(token);
        await socket.DisposeAsync();
        endpoint.FactoryRelease.TrySetResult();
        var failure = await Record.ExceptionAsync(() => setup.WaitAsync(token));
        // Accept the disposal outcome and a stop-driven cancellation, never the test's own timeout cancellation.
        (failure is ObjectDisposedException || (failure is OperationCanceledException && !token.IsCancellationRequested)).Should().BeTrue();
        endpoint.Disposals.Should().Be(1);
        endpoint.Parked.Task.IsCompleted.Should().BeFalse();
    }

    [Fact(Timeout = 20_000)]
    public async Task BindCancellationWhileFactoryRuns_ReleasesReturnedListener()
    {
        var token = TestContext.Current.CancellationToken;
        using var cancellation = new CancellationTokenSource();
        var endpoint = new ControlledEndpoint { DelayFactory = true };
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        var binding = socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, cancellation.Token);
        await endpoint.FactoryEntered.Task.WaitAsync(token);
        await cancellation.CancelAsync();
        endpoint.FactoryRelease.TrySetResult();
        await FluentActions.Awaiting(() => binding.WaitAsync(token)).Should().ThrowAsync<OperationCanceledException>()
            .Where(failure => !token.IsCancellationRequested);
        endpoint.Disposals.Should().Be(1);
        endpoint.Parked.Task.IsCompleted.Should().BeFalse();
    }

    private sealed class ReleaseOnDispose(ControlledEndpoint endpoint) : IDisposable
    {
        public void Dispose()
        {
            endpoint.Release.TrySetResult();
            endpoint.FactoryRelease.TrySetResult();
        }
    }

    private sealed class First;

    private sealed class Second;

    private sealed class ControlledEndpoint
    {
        public bool Handshake { get; init; } = true;
        public bool DelayFactory { get; init; }
        public TaskCompletionSource FactoryEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FactoryRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask WaitForFactoryAsync()
        {
            if (!DelayFactory) return;
            FactoryEntered.TrySetResult();
            await FactoryRelease.Task;
        }

        public TaskCompletionSource Parked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Stopping { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Disposals;

        public async Task ParkAsync(CancellationToken token)
        {
            Parked.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, token);
            }
            catch (OperationCanceledException)
            {
                Stopping.TrySetResult();
                await Release.Task;
                throw;
            }
        }
    }

    private sealed class ControlledTransport<T>(ControlledEndpoint endpoint)
        : IZTransport<ControlledTransport<T>, ControlledEndpoint>
    {
        public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
        {
            add { }
            remove { }
        }

        public static async ValueTask<IZConnection> ConnectAsync(ControlledEndpoint endpoint,
            CancellationToken token = default)
        {
            await endpoint.WaitForFactoryAsync();
            return new ControlledConnection(endpoint);
        }

        public static async ValueTask<ControlledTransport<T>> BindAsync(ControlledEndpoint endpoint,
            CancellationToken token = default)
        {
            await endpoint.WaitForFactoryAsync();
            return new ControlledTransport<T>(endpoint);
        }

        public ValueTask StartAsync(CancellationToken token = default) => new(endpoint.ParkAsync(token));
        public void Dispose() => Interlocked.Increment(ref endpoint.Disposals);
    }

    private sealed class ControlledConnection(ControlledEndpoint endpoint) : IZConnection
    {
        private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready());
        private int position;

        public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        {
            if (endpoint.Handshake && position < handshake.Length)
            {
                var count = Math.Min(buffer.Length, handshake.Length - position);
                handshake.AsMemory(position, count).CopyTo(buffer);
                position += count;
                return count;
            }

            await endpoint.ParkAsync(token);
            return 0;
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default) => ValueTask.CompletedTask;

        public async ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
        {
            foreach (var segment in bytes) await WriteAsync(segment, token);
        }

        public void Abort() { }

        public void Dispose() => Interlocked.Increment(ref endpoint.Disposals);
    }
}
