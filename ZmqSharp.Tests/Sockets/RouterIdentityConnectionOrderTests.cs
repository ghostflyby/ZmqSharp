using System.Buffers;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using NetMQ;
using Xunit;
using ZmqSharp;
using ZmqSharp.Tests.Interop;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Reproduces the connection-order dependency of ROUTER's local identity
/// assignment (0025 section 1): every ZRouterSocket owns an independent
/// per-socket counter and assigns a peer's routing id on first message
/// arrival, so one client gets unrelated ids on different ROUTER sockets. A
/// Jupyter kernel routes stdin back to the requesting frontend by identity;
/// with local per-socket ids that mapping depends on arrival order and
/// cross-wires two frontends' shell and stdin. The fix (0025) routes on the
/// identity peers advertise in READY, making a client's id identical on every
/// ROUTER it connects to.
/// </summary>
public sealed class RouterIdentityConnectionOrderTests
{
    [Fact]
    public async Task SameClient_AcrossTwoRouters_GetsSameAdvertisedIdentity()
    {
        // Kernel: shell and stdin are independent ROUTER sockets, mirroring a
        // Jupyter kernel's shell/control/stdin arrangement. Two frontends
        // (A, B) each connect both, each with its own READY-advertised
        // routing identity (the Jupyter client shares one identity across its
        // shell and stdin DEALER sockets).
        await using var shellRouter = new ZRouterSocket();
        await using var stdinRouter = new ZRouterSocket();
        var shellPort = GetFreePort();
        var stdinPort = GetFreePort();
        await shellRouter.BindAsync($"tcp://127.0.0.1:{shellPort}");
        await stdinRouter.BindAsync($"tcp://127.0.0.1:{stdinPort}");

        var identityA = Guid.NewGuid().ToByteArray();
        var identityB = Guid.NewGuid().ToByteArray();
        await using var aShell = new ZDealerSocket(new ZSocketOptions { Identity = identityA });
        await using var aStdin = new ZDealerSocket(new ZSocketOptions { Identity = identityA });
        await using var bShell = new ZDealerSocket(new ZSocketOptions { Identity = identityB });
        await using var bStdin = new ZDealerSocket(new ZSocketOptions { Identity = identityB });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await aShell.ConnectAsync($"tcp://127.0.0.1:{shellPort}", cts.Token);
        await bShell.ConnectAsync($"tcp://127.0.0.1:{shellPort}", cts.Token);
        await bStdin.ConnectAsync($"tcp://127.0.0.1:{stdinPort}", cts.Token);
        await aStdin.ConnectAsync($"tcp://127.0.0.1:{stdinPort}", cts.Token);

        // Shell and stdin are separate ROUTER sockets, so local counter ids
        // would differ per router (0025 section 1 repro); the advertised
        // identity must be identical on both, independent of any arrival or
        // connection order. B speaks first on stdin to prove the mapping does
        // not depend on message order either.
        await aShell.SendAsync(ZMessage.FromOwned([.. "from-A"u8]), cts.Token);
        var shellA = await ReadMessageAsync(shellRouter.Messages, "from-A", cts.Token);
        await bShell.SendAsync(ZMessage.FromOwned([.. "from-B"u8]), cts.Token);
        var shellB = await ReadMessageAsync(shellRouter.Messages, "from-B", cts.Token);

        await bStdin.SendAsync(ZMessage.FromOwned([.. "from-B"u8]), cts.Token);
        var stdinB = await ReadMessageAsync(stdinRouter.Messages, "from-B", cts.Token);
        await aStdin.SendAsync(ZMessage.FromOwned([.. "from-A"u8]), cts.Token);
        var stdinA = await ReadMessageAsync(stdinRouter.Messages, "from-A", cts.Token);

        // A frontend's routing identity is its advertised identity on every
        // ROUTER, so the kernel can route stdin back to the frontend whose
        // shell message it is answering, regardless of connection or arrival
        // order - and it is exactly the bytes the client configured.
        shellA[0].ToSequence().ToArray().Should().Equal(identityA);
        stdinA[0].ToSequence().ToArray().Should().Equal(identityA);
        shellB[0].ToSequence().ToArray().Should().Equal(identityB);
        stdinB[0].ToSequence().ToArray().Should().Equal(identityB);
    }

    [Fact]
    public async Task PeerWithoutIdentity_StillGetsLocalId_AndSendAsyncRoutesToIt()
    {
        // A peer that advertises no identity keeps the local assignment, and
        // that local id still routes outbound (the pre-0025 behavior for
        // NetMQ/libzmq peers, whose DEALERs usually send no identity).
        using var dealer = new NetMQ.Sockets.DealerSocket();
        dealer.Options.Linger = TimeSpan.Zero;
        var port = GetFreePort();
        dealer.Bind($"tcp://127.0.0.1:{port}");

        var routedMessage = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var router = new ZRouterSocket(new ZSocketOptions
        {
            MessageSink = new TestSink(message => routedMessage.TrySetResult(message))
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await router.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        dealer.SendFrame([.. "ping"u8]);
        var routed = await routedMessage.Task.WaitAsync(cts.Token);
        var identity = routed[0].ToSequence().ToArray();
        identity.Should().NotBeEmpty();
        routed[1].ToSequence().ToArray().Should().Equal([.. "ping"u8]);
        routed.Dispose();

        // The local id addresses the peer on outbound, exactly as before.
        await router.SendAsync(identity, ZMessage.FromOwned([.. "pong"u8]), cts.Token);
        var reply = InteropHelpers.ReceiveFrame(dealer, TimeSpan.FromSeconds(5));
        reply.Should().Equal([.. "pong"u8]);
    }

    [Fact]
    public async Task SecondPeer_WithInUseIdentity_IsRefused()
    {
        // libzmq ROUTER behavior: a second peer claiming an in-use identity
        // is rejected at establishment, never silently shadowed. The ZmqSharp
        // ROUTER refuses the connection, so the assertion is on the server
        // side: the second peer ends with the duplicate-identity protocol
        // error (the client may observe the teardown as a connect error or a
        // later peer end, depending on the handshake race).
        await using var router = new ZRouterSocket();
        var port = GetFreePort();
        await router.BindAsync($"tcp://127.0.0.1:{port}");

        var rejected = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        router.PeerEnded += (_, failure) => { if (failure is not null) rejected.TrySetResult(failure); };

        var identity = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var first = new ZDealerSocket(new ZSocketOptions { Identity = identity });
        await first.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await using var second = new ZDealerSocket(new ZSocketOptions { Identity = identity });
        await Record.ExceptionAsync(async () => await second.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token));

        var failure = await rejected.Task.WaitAsync(cts.Token);
        failure.Should().BeOfType<ZeroMqProtocolException>();
    }

    private static async Task<ZMessage> ReadMessageAsync(
        ChannelReader<ZMessage> reader,
        string payload,
        CancellationToken token)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        while (await reader.WaitToReadAsync(timeoutCts.Token))
        {
            while (reader.TryRead(out var message))
            {
                // ROUTER delivers [identity, payload...].
                if (message[1].ToSequence().ToArray().AsSpan().SequenceEqual(payloadBytes))
                    return message;

                message.Dispose();
            }
        }

        throw new TimeoutException($"did not receive payload '{payload}'");
    }

    private static int GetFreePort()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }
}
