using System.Buffers;
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
        Assert.True(filter.Matches(new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 })));
        Assert.False(filter.Matches(new ReadOnlySequence<byte>(new byte[] { 9, 2, 3 })));
        filter.RemoveAll([1, 2]);
        Assert.False(filter.Matches(new ReadOnlySequence<byte>(new byte[] { 1, 2, 3 })));
        filter.Add([]);
        Assert.True(filter.Matches(ReadOnlySequence<byte>.Empty));
        filter.RemoveAll([]);
        Assert.False(filter.Matches(ReadOnlySequence<byte>.Empty));
    }

    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TestTransports.TransportKinds), MemberType = typeof(TestTransports))]
    public async Task ChangedInput_DoesNotChangeFilterOrReconnectSubscription(TransportKind kind)
    {
        var endpoint = TestTransports.GetEndpoint(kind);
        await using var publisher = new ZXPubSocket();
        await using var subscriber = new ZSubSocket();
        var token = TestContext.Current.CancellationToken;
        await publisher.BindAsync(endpoint, token);
        byte[] prefix = [.. "news"u8];
        subscriber.Subscribe(prefix);
        prefix[0] = (byte)'x';
        await subscriber.ConnectAsync(endpoint, token);
        using (var subscription = await publisher.Messages.ReadAsync(token))
            Assert.Equal(new byte[] { 1, (byte)'n', (byte)'e', (byte)'w', (byte)'s' }, subscription[0].ToSequence().ToArray());
        await publisher.SendAsync("news:1"u8.ToArray(), token);
        using (var received = await subscriber.Messages.ReadAsync(token))
            Assert.Equal("news:1"u8.ToArray(), received[0].ToSequence().ToArray());

        await subscriber.DisconnectAsync(endpoint, token);
        await subscriber.ConnectAsync(endpoint, token);
        using (var subscription = await publisher.Messages.ReadAsync(token))
            Assert.Equal(new byte[] { 1, (byte)'n', (byte)'e', (byte)'w', (byte)'s' }, subscription[0].ToSequence().ToArray());
        subscriber.Unsubscribe("news"u8);
        using var unsubscription = await publisher.Messages.ReadAsync(token);
        Assert.Equal(new byte[] { 0, (byte)'n', (byte)'e', (byte)'w', (byte)'s' }, unsubscription[0].ToSequence().ToArray());
    }
}
