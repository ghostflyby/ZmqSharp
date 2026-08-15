using System.Buffers;
using System.Net.Sockets;
using System.Threading.Channels;
using FluentAssertions;
using Xunit;
using ZmqSharp.Patterns;
using ZmqSharp.Transports;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Custom socket types (0015 section 2.3): a type outside the built-in set
/// interoperates only between ZmqSharp endpoints advertising the same name -
/// the NetMQ interop suite cannot validate it, so it is validated in-library
/// with pair tests over both transports (0015 section 5.4). The custom
/// endpoint composes a <see cref="ZSocketType"/> whose predicate accepts only
/// the same name.
/// </summary>
public sealed class CustomSocketTypeTests
{
    private const string CustomName = "FOO";

    [Fact]
    public void CustomSocketBaseSubclass_WithQueueOptions_Throws()
    {
        // A custom ZSocketBase subclass never composes a queue; queue options
        // on it are rejected at construction (0023).
        var act = () => new CustomTypeSocket(CustomName, new ZSocketOptions
        {
            ReceiveQueueFactory = new BoundedChannelOptions(4) { SingleWriter = true }
        });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*never composes a queue*");
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CustomType_Pair_HandshakeCompletesAndMessagesRoundTrip(TransportKind kind)
    {
        var endpoint = TestTransports.GetEndpoint(kind);
        var received = new TaskCompletionSource<ZMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new CustomTypeSocket(CustomName, new ZSocketOptions { MessageSink = new TestSink(message => received.TrySetResult(message)) });
        await using var client = new CustomTypeSocket(CustomName);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await server.BindAsync(endpoint, cts.Token);
        await client.ConnectAsync(endpoint, cts.Token);

        await client.SendAsync(ZMessage.FromOwned([.. "hello"u8]), cts.Token);

        var message = await received.Task.WaitAsync(cts.Token);
        byte[] expected = [.. "hello"u8];
        message[0].ToSequence().ToArray().Should().Equal(expected);
        message.Dispose();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task CustomType_BuiltInPeer_HandshakeRejected(TransportKind kind)
    {
        // A custom endpoint never accepts a built-in peer (different name), and
        // a built-in endpoint never accepts a custom peer: the READY Socket-Type
        // does not match either predicate, so establishment fails with the
        // protocol rejection (RFC 23 ERROR + ZeroMqProtocolException).
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var server = new ZPairSocket();
        await using var client = new CustomTypeSocket(CustomName);

        await server.BindAsync(endpoint);
        var failure = await Record.ExceptionAsync(() => client.ConnectAsync(endpoint));

        failure.Should().NotBeNull();
        // Both peers reject each other. Either the local rejection surfaces as
        // ZeroMqProtocolException, or the peer's rejection wins the race and
        // closes first, so the local ERROR write faults with an IO error
        // (broken pipe on ipc, buffered write on tcp). When the protocol
        // rejection is the one that surfaces, its semantics must still name
        // the socket-type mismatch.
        (failure is ZeroMqProtocolException or IOException or SocketException).Should().BeTrue();
        if (failure is ZeroMqProtocolException)
            failure.Message.Should().Contain("not accepted by local socket type");
    }

    /// <summary>
    /// A test composition root binding a custom <see cref="ZSocketType"/> with
    /// a pair-shaped single-peer dispatch (0015 section 2.1 / 0019). The
    /// composition face is the protected base constructor.
    /// </summary>
    private sealed class CustomTypeSocket(string name, ZSocketOptions? options = null) : ZSocketBase(
        options ?? new ZSocketOptions(),
        new ZSinglePeerDispatch(),
        ZSocketType.ForCustom(name))
    {
        public ValueTask SendAsync(ZMessage message, CancellationToken token = default)
        {
            return SendAsyncCore(message, token);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        {
            return SendAsyncCore(bytes, token);
        }
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
