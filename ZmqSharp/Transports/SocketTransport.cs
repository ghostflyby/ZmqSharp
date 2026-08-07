using System.Net;
using System.Net.Sockets;

namespace ZmqSharp.Transports;

/// <summary>
/// Socket-based transport: ConnectAsync yields a connected connection; BindAsync
/// yields a listening transport that reports accepted peers via OnAccept.
/// </summary>
public sealed class SocketTransport : IZTransport<SocketTransport, EndPoint>
{
    private readonly Socket socket;
    private Func<IZConnection, CancellationToken, ValueTask>? onAccept;
    private int closed;

    private SocketTransport(Socket socket)
    {
        this.socket = socket;
    }

    public event Func<IZConnection, CancellationToken, ValueTask>? OnAccept
    {
        add => onAccept += value;
        remove => onAccept -= value;
    }

    public static async ValueTask<IZConnection> ConnectAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(endpoint, token);
            socket.NoDelay = true;
            return new ZConnection(new NetworkStream(socket, ownsSocket: true));
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public static ValueTask<SocketTransport> BindAsync(
        EndPoint endpoint,
        ZTransportOptions options,
        CancellationToken token = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(endpoint);
        socket.Listen();
        return ValueTask.FromResult(new SocketTransport(socket));
    }

    public async ValueTask StartAsync(CancellationToken token = default)
    {
        while (Volatile.Read(ref closed) == 0)
        {
            Socket accepted;
            try
            {
                accepted = await socket.AcceptAsync(token);
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }

            accepted.NoDelay = true;
            var connection = new ZConnection(new NetworkStream(accepted, ownsSocket: true));
            if (onAccept is not null)
            {
                await onAccept(connection, token);
            }
            else
            {
                connection.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref closed, 1) == 0)
        {
            socket.Dispose();
        }
    }
}
