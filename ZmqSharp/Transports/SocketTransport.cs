using System.Net;
using System.Net.Sockets;
using ZmqSharp.Sockets;

namespace ZmqSharp.Transports;

/// <summary>
/// Socket-based transport: connected transports expose a NetworkStream; bound
/// transports act as listeners (AcceptAsync yields connected transports).
/// The API differs from TCP only by the concrete endpoint type, so ZMQ IPC
/// (Unix domain sockets) plugs in the same way with its own endpoint type;
/// the factory takes the abstract EndPoint and the Stream mechanics are shared.
/// </summary>
public sealed class SocketTransport : IZTransport<SocketTransport, EndPoint>
{
    private readonly Socket socket;
    private readonly bool listening;

    public Stream? Stream { get; }

    private SocketTransport(Socket socket, bool listening)
    {
        this.socket = socket;
        this.listening = listening;
        if (!listening)
        {
            this.socket.NoDelay = true;
            Stream = new NetworkStream(socket, ownsSocket: true);
        }
    }

    public static async ValueTask<SocketTransport> ConnectAsync(
        IZSocket zsocket,
        EndPoint endpoint,
        CancellationToken token = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(endpoint, token);
            return new SocketTransport(socket, listening: false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public static ValueTask<SocketTransport> BindAsync(
        IZSocket zsocket,
        EndPoint endpoint,
        CancellationToken token = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(endpoint);
        socket.Listen();
        return ValueTask.FromResult(new SocketTransport(socket, listening: true));
    }

    public async ValueTask<IZTransport> AcceptAsync(CancellationToken token = default)
    {
        if (!listening)
        {
            throw new InvalidOperationException("transport is not listening");
        }

        var socket = await this.socket.AcceptAsync(token);
        return new SocketTransport(socket, listening: false);
    }

    public void Dispose() => socket.Dispose();
}
