using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
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
        var token = TestContext.Current.CancellationToken;
        var socket = new ZPairSocket();
        await socket.BindAsync($"ipc://{path}", token);

        Assert.True(File.Exists(path));

        await socket.DisposeAsync();
        Assert.False(File.Exists(path));
    }

    [Theory]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Bind_AfterDispose_SamePathSucceeds(string path)
    {
        var token = TestContext.Current.CancellationToken;
        await using (var first = new ZPairSocket())
        {
            await first.BindAsync($"ipc://{path}", token);
        }

        // The unlink on dispose freed the path, so a later bind of the same
        // path must succeed instead of failing with EADDRINUSE.
        await using var second = new ZPairSocket();
        await second.BindAsync($"ipc://{path}", token);
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Connect_MissingPath_ThrowsCleanly(string path)
    {
        var token = TestContext.Current.CancellationToken;
        await using var client = new ZPairSocket();

        var failure = await Record.ExceptionAsync(() => client.ConnectAsync($"ipc://{path}", token).WaitAsync(token));
        Assert.NotNull(failure);
        Assert.True(failure is SocketException or IOException);
    }

    [Fact]
    public async Task Bind_RelativePath_ResolvesToTempDirectory()
    {
        // The URI parser puts a relative form such as "ipc://name.sock" in the
        // host slot; it must resolve against the system temp directory instead
        // of the filesystem root (0020 section 3).
        var name = $"zmqsharp-rel-{Guid.NewGuid().ToString("N")[..8]}.sock";
        var token = TestContext.Current.CancellationToken;
        var socket = new ZPairSocket();
        await socket.BindAsync($"ipc://{name}", token);
        try
        {
            Assert.True(File.Exists(Path.Combine(Path.GetTempPath(), name)));
        }
        finally
        {
            await socket.DisposeAsync();
        }
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.IpcPaths), MemberType = typeof(TestTransports))]
    public async Task Push_FansOutToMultiplePullPeers(string pathA)
    {
        var endpointA = $"ipc://{pathA}";
        var endpointB = $"ipc://{TestTransports.IpcSocketPath("zmqsharp-test-")}";
        await using var pullA = new ZPullSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var pullB = new ZPullSocket(new ZSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true } });
        await using var push = new ZPushSocket();
        var token = TestContext.Current.CancellationToken;
        using var pump = CancellationTokenSource.CreateLinkedTokenSource(token);

        await pullA.BindAsync(endpointA, token);
        await pullB.BindAsync(endpointB, token);
        await push.ConnectAsync(endpointA, token);
        await push.ConnectAsync(endpointB, token);

        var countA = 0;
        var countB = 0;
        var bothReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainA = DrainAsync(pullA, () => OnPeerMessage(true), pump.Token);
        var drainB = DrainAsync(pullB, () => OnPeerMessage(false), pump.Token);

        // The push drops sends to peers that have not finished the handshake,
        // so keep sending until both drains report a message.
        for (var attempt = 0; attempt < 100 && !bothReached.Task.IsCompleted; attempt++)
        {
            await push.SendAsync(ZMessage.FromOwned([.. "work"u8]), token);
            if (await Task.WhenAny(bothReached.Task, Task.Delay(25, token)) == bothReached.Task) break;
        }

        await bothReached.Task.WaitAsync(token);
        Assert.True(countA >= 1, $"countA was {countA}, expected at least 1");
        Assert.True(countB >= 1, $"countB was {countB}, expected at least 1");

        await pump.CancelAsync();
        try
        {
            await Task.WhenAll(drainA, drainB);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }

        return;

        void OnPeerMessage(bool peerA)
        {
            if (peerA)
                Interlocked.Increment(ref countA);
            else
                Interlocked.Increment(ref countB);

            if (Volatile.Read(ref countA) >= 1 && Volatile.Read(ref countB) >= 1) bothReached.TrySetResult();
        }
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.AbstractNames), MemberType = typeof(TestTransports))]
    public async Task AbstractNamespace_RoundTripsWithoutFilesystemEntry(string name)
    {
        // The Linux abstract namespace (libzmq's ipc://@name convention, 0020):
        // the address lives in the kernel namespace, creates no filesystem
        // entry, and is cleaned up when the socket closes - no unlink needed.
        // It exists only on Linux; off Linux the '@' form is a literal path
        // and the scenario does not apply.
        if (!OperatingSystem.IsLinux()) return;

        var endpoint = $"ipc://@{name}";
        var token = TestContext.Current.CancellationToken;
        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions { MessageSink = new TestSink(message => received.TrySetResult(message)) });
        await server.BindAsync(endpoint, token);

        Assert.False(File.Exists(Path.Combine(Path.GetTempPath(), name)));

        await using var client = new ZPairSocket();
        await client.ConnectAsync(endpoint, token);
        await client.SendAsync(ZMessage.FromOwned([.. "hi"u8]), token);

        var message = await received.Task.WaitAsync(token);
        Assert.Equal((byte[])[.. "hi"u8], message[0].ToSequence().ToArray());
        message.Dispose();

        // Disposing the bound socket cleans up the abstract address implicitly.
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }

    private static async Task DrainAsync(ZQueueSocketBase socket, Action onMessage,
        CancellationToken token)
    {
        await foreach (var message in socket.Messages.ReadAllAsync(token))
        {
            onMessage();
            message.Dispose();
        }
    }
}
