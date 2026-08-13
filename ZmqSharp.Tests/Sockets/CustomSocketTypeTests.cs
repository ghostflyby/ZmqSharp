using System.Buffers;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Custom socket types (0015 section 2.3): a type outside the built-in set
/// interoperates only between ZmqSharp endpoints advertising the same name -
/// the NetMQ interop suite cannot validate it, so it is validated in-library
/// with pair tests over TCP. The custom endpoint composes a
/// <see cref="ZSocketType"/> whose predicate accepts only the same name.
/// </summary>
public sealed class CustomSocketTypeTests
{
    private const string CustomName = "FOO";

    [Fact]
    public async Task CustomType_Pair_HandshakeCompletesAndMessagesRoundTrip()
    {
        var port = GetFreePort();
        await using var server = new CustomTypeSocket(CustomName);
        await using var client = new CustomTypeSocket(CustomName);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        server.BindMessageSink(new TestSink(message => received.TrySetResult(message)));

        await server.BindAsync($"tcp://127.0.0.1:{port}", cts.Token);
        await client.ConnectAsync($"tcp://127.0.0.1:{port}", cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);

        var message = await received.Task.WaitAsync(cts.Token);
        byte[] expected = [.. "hello"u8];
        message[0].ToSequence().ToArray().Should().Equal(expected);
        message.Dispose();
    }

    [Fact]
    public async Task CustomType_BuiltInPeer_HandshakeRejected()
    {
        // A custom endpoint never accepts a built-in peer (different name), and
        // a built-in endpoint never accepts a custom peer: the READY Socket-Type
        // does not match either predicate, so establishment fails with the
        // protocol rejection (RFC 23 ERROR + ZeroMqProtocolException).
        var port = GetFreePort();
        await using var server = ZSocket.CreatePairCallback();
        await using var client = new CustomTypeSocket(CustomName);

        await server.BindAsync($"tcp://127.0.0.1:{port}");
        var act = async () => await client.ConnectAsync($"tcp://127.0.0.1:{port}");

        await act.Should().ThrowAsync<ZeroMqProtocolException>().WithMessage("*not accepted by local socket type*");
    }

    /// <summary>
    /// A test composition root binding a custom <see cref="ZSocketType"/> with
    /// a pair-shaped single-peer dispatch (0015 section 2.1 / 0019). The
    /// composition face is the protected base constructor.
    /// </summary>
    private sealed class CustomTypeSocket(string name) : ZSocketBase(
        new ZSocketOptions(),
        new ZSinglePeerDispatch(),
        ZSocketType.ForCustom(name))
    {
    }

    private sealed class TestSink(Action<ZMessage> onMessage) : IPatternSink
    {
        public ValueTask OnMessageAsync(IZConnection peer, ZMessage message, CancellationToken token = default)
        {
            onMessage(message);
            return ValueTask.CompletedTask;
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
