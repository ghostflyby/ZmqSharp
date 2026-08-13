using System.Net;
using System.Net.Sockets;

namespace ZmqSharp.Transports;

/// <summary>
/// Socket-based transport: ConnectAsync yields a connected connection; BindAsync
/// yields listening transport that reports accepted peers via OnAccept.
/// The transport is endpoint-agnostic (0015 section 5.1): the socket is constructed
/// from the endpoint's address family, so both TCP (<see cref="IPEndPoint"/>) and
/// Unix domain (ipc, <see cref="UnixDomainSocketEndPoint"/>) endpoints flow
/// through the same transport. Connections are <see cref="ZSocketConnection"/>
/// instances reading directly from the socket (0015 section 4).
/// </summary>
public sealed class SocketTransport : IZTransport<SocketTransport, EndPoint>
{
    private readonly Socket socket;
    private readonly string? boundPath;
    private Func<IZConnection, CancellationToken, ValueTask>? onAccept;
    private int closed;

    private SocketTransport(Socket socket, string? boundPath = null)
    {
        this.socket = socket;
        this.boundPath = boundPath;
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
        var socket = CreateSocket(endpoint);
        try
        {
            await socket.ConnectAsync(endpoint, token);
            SetTcpOptions(socket, endpoint);
            return new ZSocketConnection(socket);
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
        var socket = CreateSocket(endpoint);
        socket.Bind(endpoint);
        socket.Listen();
        // net10 UnixDomainSocketEndPoint exposes no Path/FilePath property;
        // ToString() is the public way to get the bound path back to
        // unlink on dispose (verified for non-abstract paths, which is all the
        // ipc facade produces).
        var boundPath = endpoint is UnixDomainSocketEndPoint ? endpoint.ToString() : null;
        return ValueTask.FromResult(new SocketTransport(socket, boundPath));
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

            try
            {
                SetTcpOptions(accepted, accepted.RemoteEndPoint ?? socket.LocalEndPoint);
                var connection = new ZSocketConnection(accepted);
                if (onAccept is not null)
                    await onAccept(connection, token);
                else
                    connection.Dispose();
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
            {
                // The peer reset before the connection could be set up; drop it
                // without faulting the accept loop.
                accepted.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;

        socket.Dispose();
        // Unix domain bind creates a filesystem entry for the path; remove it
        // so a later bind of the same path succeeds (0015 section 5.2).
        // Abstract-namespace addresses (displayed with a leading '@', libzmq's
        // convention, 0020) have no filesystem entry - they are cleaned up
        // when the socket closes, and a File.Delete would target a wrong path
        // (a file literally named "@...") at best.
        if (boundPath is not null && !boundPath.StartsWith('@')) File.Delete(boundPath);
    }

    /// <summary>
    /// Constructs the socket from the endpoint's address family instead of
    /// hard-coding TCP: InterNetwork endpoints keep the previous behavior, and
    /// Unix endpoints (ipc) get an AF_UNIX stream socket.
    /// </summary>
    private static Socket CreateSocket(EndPoint endpoint)
    {
        return new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
    }

    /// <summary>TCP-only tuning: NoDelay is not valid on a Unix domain socket.</summary>
    private static void SetTcpOptions(Socket socket, EndPoint? endpoint)
    {
        if (endpoint is not null && endpoint.AddressFamily != AddressFamily.Unix) socket.NoDelay = true;
    }
}
