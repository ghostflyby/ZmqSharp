using System.Buffers;
using FluentAssertions;
using Xunit;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests.Sockets;

public sealed class SubscriptionOwnershipTests
{
    [Fact]
    public void Filter_CopiesThePrefix_AndUnsubscribesByContent()
    {
        byte[] input = [1, 2];
        var filter = new ZTopicFilter();
        filter.Add(input);
        input[0] = 9;
        filter.Matches(new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 })).Should().BeTrue();
        filter.Matches(new ReadOnlySequence<byte>(new byte[] { 9, 2, 3 })).Should().BeFalse();
        filter.RemoveAll([1, 2]);
        filter.Matches(new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 })).Should().BeFalse();
        filter.Add([]);
        filter.Matches(ReadOnlySequence<byte>.Empty).Should().BeTrue();
        filter.RemoveAll([]);
        filter.Matches(ReadOnlySequence<byte>.Empty).Should().BeFalse();
    }

    [Theory]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ChangedInput_DoesNotChangeFilterOrReconnectSubscription(TransportKind kind)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var publisher = new ZXPubSocket();
        await using var subscriber = new ZSubSocket();
        await publisher.BindAsync(endpoint, timeout.Token);
        byte[] prefix = [.. "news"u8];
        subscriber.Subscribe(prefix);
        prefix[0] = (byte)'x';
        await subscriber.ConnectAsync(endpoint, timeout.Token);
        using (var subscription = await publisher.Messages.ReadAsync(timeout.Token))
            subscription[0].ToSequence().ToArray().Should().Equal(new byte[] { 1, (byte)'n', (byte)'e', (byte)'w', (byte)'s' });
        await publisher.SendAsync("news:1"u8.ToArray(), timeout.Token);
        using (var received = await subscriber.Messages.ReadAsync(timeout.Token))
            received[0].ToSequence().ToArray().Should().Equal("news:1"u8.ToArray());

        await subscriber.DisconnectAsync(endpoint, timeout.Token);
        await subscriber.ConnectAsync(endpoint, timeout.Token);
        using (var subscription = await publisher.Messages.ReadAsync(timeout.Token))
            subscription[0].ToSequence().ToArray().Should().Equal(new byte[] { 1, (byte)'n', (byte)'e', (byte)'w', (byte)'s' });
        subscriber.Unsubscribe("news"u8);
        using var unsubscription = await publisher.Messages.ReadAsync(timeout.Token);
        unsubscription[0].ToSequence().ToArray().Should().Equal(new byte[] { 0, (byte)'n', (byte)'e', (byte)'w', (byte)'s' });
    }
}
