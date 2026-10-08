using System.Buffers;
using ZmqSharp.Patterns;
using ZmqSharp.Sockets;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp;

/// <summary>Socket surface over a runtime that owns peer sessions and lifecycle.</summary>
public abstract class ZSocketBase : IZSocket
{
    private protected SocketRuntime Runtime { get; }
    protected MemoryPool<byte> Pool => Runtime.Pool;
    protected Lock StateLock => Runtime.StateLock;

    protected ZSocketBase(ZSocketOptions options, IZDispatchPolicy dispatch, ZSocketType type,
        IZInboundPolicy? inbound = null) : this(new SocketRuntime(options, dispatch, type, inbound)) { }

    private protected ZSocketBase(SocketRuntime runtime)
    {
        Runtime = runtime;
        runtime.PeerEstablished += OnPeerEstablished;
    }

    protected virtual void OnPeerEstablished(ZPeer peer, ReadOnlyMemory<byte>? advertisedIdentity) { }
    protected void ThrowIfClosed() => Runtime.ThrowIfClosed();
    protected void TrackBackground(Task task) => Runtime.TrackBackground(task);
    internal ZPeer[] PeerSnapshot => Runtime.PeerSnapshot;
    internal void SetPeerConnectedHandler(Action<ZPeer> handler) => Runtime.SetPeerConnectedHandler(handler);

    internal ValueTask SendToAsync(ZPeer peer, ZMessage message, CancellationToken token = default)
        => Runtime.SendToAsync(peer, message, token);

    protected ValueTask SendAsyncCore(ZMessage message, CancellationToken token = default)
        => Runtime.SendAsyncCore(message, token);

    protected ValueTask SendAsyncCore(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => Runtime.SendAsyncCore(bytes, token);

    internal long ReceiveRejectionsCount => Runtime.ReceiveRejectionsCount;

    internal event Action? MaterializerRejected
    {
        add => Runtime.MaterializerRejected += value;
        remove => Runtime.MaterializerRejected -= value;
    }

    public event ZFrameHandler? OnFrame
    {
        add => Runtime.OnFrame += value;
        remove => Runtime.OnFrame -= value;
    }

    public event Action<ZPeer, Exception?>? PeerEnded
    {
        add => Runtime.PeerEnded += value;
        remove => Runtime.PeerEnded -= value;
    }

    public void ResumePaused() => Runtime.ResumePaused();

    public Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.BindAsync<TEndpoint, TTransport>(endpoint, token);

    public Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.ConnectAsync<TEndpoint, TTransport>(endpoint, token);

    public void Unbind<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.Unbind<TEndpoint, TTransport>(endpoint);

    public void Disconnect<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.Disconnect<TEndpoint, TTransport>(endpoint);

    public ValueTask UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.UnbindAsync<TEndpoint, TTransport>(endpoint, token);

    public ValueTask DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint> => Runtime.DisconnectAsync<TEndpoint, TTransport>(endpoint, token);

    public Task BindAsync(string endpoint, CancellationToken token = default) => Runtime.BindAsync(endpoint, token);
    public Task ConnectAsync(string endpoint, CancellationToken token = default) => Runtime.ConnectAsync(endpoint, token);
    public ValueTask UnbindAsync(string endpoint, CancellationToken token = default) => Runtime.UnbindAsync(endpoint, token);
    public ValueTask DisconnectAsync(string endpoint, CancellationToken token = default) => Runtime.DisconnectAsync(endpoint, token);
    public virtual ValueTask DisposeAsync() => Runtime.DisposeAsync();
}
