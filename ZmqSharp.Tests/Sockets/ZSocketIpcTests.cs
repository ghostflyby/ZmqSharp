using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// ipc (Unix domain socket) lifecycle and fan-out tests (0015 section 5):
/// bind-path unlink on dispose, rebinding a freed path, connecting to a
/// missing path, and multi-peer fan-out. The transport behavior itself is
/// covered by the transport-parameterized suites (0015 section 5.4); these
/// cover the ipc-specific surface. They run on every platform: ZmqSharp's
/// ipc is real AF_UNIX on Windows 10 1803+ too (0020).
/// </summary>
public sealed class ZSocketIpcTests
{
    [Theory]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Bind_UnlinksPath_OnSocketDispose(string path)
    {
        var socket = ZSocket.CreatePairCallback();
        await socket.BindAsync($"ipc://{path}");

        File.Exists(path).Should().BeTrue("binding an ipc endpoint creates the filesystem entry");

        await socket.DisposeAsync();
        File.Exists(path).Should().BeFalse("disposing the bound socket unlinks the path (0015 section 5.2)");
    }

    [Theory]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Bind_AfterDispose_SamePathSucceeds(string path)
    {
        await using (var first = ZSocket.CreatePairCallback())
        {
            await first.BindAsync($"ipc://{path}");
        }

        // The unlink on dispose freed the path, so a later bind of the same
        // path must succeed instead of failing with EADDRINUSE.
        await using var second = ZSocket.CreatePairCallback();
        await FluentActions.Awaiting(() => second.BindAsync($"ipc://{path}")).Should().NotThrowAsync();
    }

    [Theory]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Connect_MissingPath_ThrowsCleanly(string path)
    {
        await using var client = ZSocket.CreatePairCallback();

        var failure = await Record.ExceptionAsync(
            () => client.ConnectAsync($"ipc://{path}").WaitAsync(TimeSpan.FromSeconds(5)));
        failure.Should().NotBeNull();
        (failure is SocketException or IOException).Should().BeTrue();
    }

    [Fact]
    public async Task Bind_RelativePath_ResolvesToTempDirectory()
    {
        // The URI parser puts a relative form such as "ipc://name.sock" in the
        // host slot; it must resolve against the system temp directory instead
        // of the filesystem root (0020 section 3).
        var name = $"zmqsharp-rel-{Guid.NewGuid().ToString("N")[..8]}.sock";
        var socket = ZSocket.CreatePairCallback();
        await socket.BindAsync($"ipc://{name}");
        try
        {
            File.Exists(Path.Combine(Path.GetTempPath(), name)).Should().BeTrue(
                "a relative ipc path resolves against the system temp directory");
        }
        finally
        {
            await socket.DisposeAsync();
        }
    }

    [Theory]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Push_FansOutToMultiplePullPeers(string pathA)
    {
        var endpointA = $"ipc://{pathA}";
        var endpointB = $"ipc://{TestTransports.IpcSocketPath("zmqsharp-test-")}";
        await using var pullA = ZSocket.CreatePull(new ZQueueSocketOptions
        { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var pullB = ZSocket.CreatePull(new ZQueueSocketOptions
        { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var push = ZSocket.CreatePush();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await pullA.BindAsync(endpointA, cts.Token);
        await pullB.BindAsync(endpointB, cts.Token);
        await push.ConnectAsync(endpointA, cts.Token);
        await push.ConnectAsync(endpointB, cts.Token);

        var countA = 0;
        var countB = 0;
        var bothReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainA = DrainAsync(pullA, () => OnPeerMessage(true), cts.Token);
        var drainB = DrainAsync(pullB, () => OnPeerMessage(false), cts.Token);

        // The push drops sends to peers that have not finished the handshake,
        // so keep sending until both drains report a message.
        for (var attempt = 0; attempt < 100 && !bothReached.Task.IsCompleted; attempt++)
        {
            await push.SendAsync(ZMessage.FromOwned([.. "work"u8]), cts.Token);
            if (await Task.WhenAny(bothReached.Task, Task.Delay(25, cts.Token)) == bothReached.Task) break;
        }

        await bothReached.Task.WaitAsync(cts.Token);
        countA.Should().BeGreaterThanOrEqualTo(1);
        countB.Should().BeGreaterThanOrEqualTo(1);

        void OnPeerMessage(bool peerA)
        {
            if (peerA)
                Interlocked.Increment(ref countA);
            else
                Interlocked.Increment(ref countB);

            if (Volatile.Read(ref countA) >= 1 && Volatile.Read(ref countB) >= 1) bothReached.TrySetResult();
        }

        await cts.CancelAsync();
        try
        {
            await Task.WhenAll(drainA, drainB);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task DrainAsync<TSocket>(ZQueueSocket<TSocket> socket, Action onMessage,
        CancellationToken token)
        where TSocket : ZSocketBase
    {
        await foreach (var message in socket.Messages.ReadAllAsync(token))
        {
            onMessage();
            message.Dispose();
        }
    }
}
