using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// Common contract shared by all socket types: bind/connect to endpoints over
/// any transport, manage one or more peer connections, send and receive.
/// Socket types differ only in the scheduling policy.
/// </summary>
public interface IZSocket : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task CloseAsync(CancellationToken token = default);

    /// <summary>
    /// Low-level streaming callback layer: invoked once per frame as it arrives
    /// (borrowed, valid only during the call). Return false to pause this
    /// connection until the socket resumes it (backpressure).
    /// </summary>
    event ZFrameHandler? OnFrame;

    /// <summary>Receive channel layer with assembled messages; non-null only when a receive capacity is configured.</summary>
    ChannelReader<IZMessage>? Messages { get; }

    /// <summary>
    /// Direct send: transfers ownership of <paramref name="message"/>; the socket
    /// disposes it after routing.
    /// </summary>
    ValueTask SendAsync(IZMessage message, CancellationToken token = default);

    /// <summary>Direct send: copies the payload into an owned message before routing.</summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);

    /// <summary>
    /// Non-blocking send via the optional send channel. Returns false when no
    /// send channel is configured or the queue is full; on false the caller
    /// keeps ownership of the message.
    /// </summary>
    bool TrySend(IZMessage message);

    /// <summary>Optional send channel; non-null only when a send capacity is configured.</summary>
    ChannelWriter<IZMessage>? Outbound { get; }
}
