using System.Buffers;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

public sealed class EndpointLifecycleTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stop_WaitsForCleanupAndRepeatedCallsShareCompletion(bool listener)
    {
        var endpoint = new ControlledEndpoint();
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        if (listener) await socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        else await socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.Parked.Task.WaitAsync(Timeout);
        if (listener) socket.Unbind<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        else socket.Disconnect<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.Stopping.Task.WaitAsync(Timeout);
        var first = listener
            ? socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint).AsTask()
            : socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint).AsTask();
        var second = listener
            ? socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint).AsTask()
            : socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint).AsTask();
        first.IsCompleted.Should().BeFalse();
        second.IsCompleted.Should().BeFalse();
        endpoint.Release.TrySetResult();
        await Task.WhenAll(first, second).WaitAsync(Timeout);
        endpoint.Disposals.Should().Be(1);
        socket.PeerSnapshot.Should().BeEmpty();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationOnlyCancelsWait_AndTransportTypeIsPartOfIdentity(bool listener)
    {
        var endpoint = new ControlledEndpoint();
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        if (listener) await socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        else await socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.Parked.Task.WaitAsync(Timeout);
        if (listener) await socket.UnbindAsync<ControlledEndpoint, ControlledTransport<Second>>(endpoint);
        else await socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<Second>>(endpoint);
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
        await endpoint.Stopping.Task.WaitAsync(Timeout);
        await waitCancellation.CancelAsync();
        await FluentActions.Awaiting(() => waiting).Should().ThrowAsync<OperationCanceledException>();
        endpoint.Release.TrySetResult();
        if (listener) await socket.UnbindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        else await socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        endpoint.Disposals.Should().Be(1);
    }

    [Fact]
    public async Task DisconnectDuringHandshake_WaitsForItsCleanup()
    {
        var endpoint = new ControlledEndpoint { Handshake = false };
        await using var socket = new ZPairSocket(new ZSocketOptions { HandshakeTimeoutMs = 0 });
        using var cleanup = new ReleaseOnDispose(endpoint);
        var connecting = socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.Parked.Task.WaitAsync(Timeout);
        var disconnecting = socket.DisconnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint).AsTask();
        await endpoint.Stopping.Task.WaitAsync(Timeout);
        disconnecting.IsCompleted.Should().BeFalse();
        endpoint.Release.TrySetResult();
        await disconnecting.WaitAsync(Timeout);
        await FluentActions.Awaiting(() => connecting).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task UnknownStringEndpoint_DoesNotResolveDns()
    {
        await using IZSocket socket = new ZPairSocket();
        await socket.UnbindAsync("tcp://does-not-exist.invalid:12345");
        await socket.DisconnectAsync("tcp://does-not-exist.invalid:12345");
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task StringEndpoints_UnbindPreservesAcceptedPeer_AndDisconnectWaitsForEnded(TransportKind kind)
    {
        var address = TestTransports.GetEndpoint(kind);
        await using var server = new ZPairSocket();
        await using var client = new ZPairSocket();
        await server.BindAsync(address);
        await client.ConnectAsync(address);
        await server.UnbindAsync(address);
        await client.SendAsync("still connected"u8.ToArray());
        using var message = await server.Messages.ReadAsync().AsTask().WaitAsync(Timeout);
        message[0].ToSequence().ToArray().Should().Equal("still connected"u8.ToArray());
        var ended = false;
        client.PeerEnded += (_, _) => ended = true;
        await client.DisconnectAsync(address);
        ended.Should().BeTrue();
        client.PeerSnapshot.Should().BeEmpty();
        await client.DisconnectAsync(address);
        if (kind == TransportKind.Ipc)
        {
            await server.BindAsync(address);
            await server.UnbindAsync(address);
        }
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task DisconnectWithFullReceiveQueue_CancelsBackpressureAndReclaimsBuffers(TransportKind kind)
    {
        var address = TestTransports.GetEndpoint(kind);
        using var timeout = new CancellationTokenSource(Timeout);
        using var pool = new CountingMemoryPool();
        await using var server = new ZPairSocket();
        await using var receiver = new ZPairSocket(new ZSocketOptions
        {
            Pool = pool,
            ReceiveQueueFactory = new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait }
        });
        await server.BindAsync(address, timeout.Token);
        await receiver.ConnectAsync(address, timeout.Token);
        var baseline = pool.Outstanding;
        await server.SendAsync("first"u8.ToArray(), timeout.Token);
        await server.SendAsync("second"u8.ToArray(), timeout.Token);
        await pool.WaitForOutstandingAtLeastAsync(baseline + 2, Timeout);
        await receiver.DisconnectAsync(address, timeout.Token);
        pool.Outstanding.Should().Be(0);
        receiver.Messages.TryRead(out _).Should().BeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FactoryReturnsAfterSocketCloses_ResourceIsReleasedWithoutStartingPump(bool listening)
    {
        var endpoint = new ControlledEndpoint { DelayFactory = true };
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        var setup = listening
            ? socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint)
            : socket.ConnectAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint);
        await endpoint.FactoryEntered.Task.WaitAsync(Timeout);
        await socket.DisposeAsync();
        endpoint.FactoryRelease.TrySetResult();
        var failure = await Record.ExceptionAsync(() => setup.WaitAsync(Timeout));
        (failure is ObjectDisposedException or OperationCanceledException).Should().BeTrue();
        endpoint.Disposals.Should().Be(1);
        endpoint.Parked.Task.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task BindCancellationWhileFactoryRuns_ReleasesReturnedListener()
    {
        using var cancellation = new CancellationTokenSource();
        var endpoint = new ControlledEndpoint { DelayFactory = true };
        await using var socket = new ZPairSocket();
        using var cleanup = new ReleaseOnDispose(endpoint);
        var binding = socket.BindAsync<ControlledEndpoint, ControlledTransport<First>>(endpoint, cancellation.Token);
        await endpoint.FactoryEntered.Task.WaitAsync(Timeout);
        await cancellation.CancelAsync();
        endpoint.FactoryRelease.TrySetResult();
        await FluentActions.Awaiting(() => binding.WaitAsync(Timeout)).Should().ThrowAsync<OperationCanceledException>();
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
                await Task.Delay(System.Threading.Timeout.Infinite, token);
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
        private readonly byte[] handshake = ZmtpTestData.Concat(ZmtpTestData.Greeting(), ZmtpTestData.Ready("PAIR"));
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

        public async ValueTask WriteAsync(System.Buffers.ReadOnlySequence<byte> bytes, CancellationToken token = default)
        {
            foreach (var segment in bytes) await WriteAsync(segment, token);
        }

        public void Abort() { }

        public void Dispose() => Interlocked.Increment(ref endpoint.Disposals);
    }
}
