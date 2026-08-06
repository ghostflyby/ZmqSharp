using System.Buffers;
using System.Net;
using System.Net.Sockets;

namespace ZmqSharp;

public interface IZSocket
{
    Task Connect<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    Task Bind<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    ValueTask Send(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask Send(ReadOnlySequence<byte> sequence, CancellationToken token = default);

    event Func<ReadOnlySequence<byte>, CancellationToken, bool> OnReceive;

    MemoryPool<byte> MemoryPool { get; }
}

public interface IZTransport
{
    public IZSocket ZSocket { get; init; }

    ValueTask<int> Send(ReadOnlyMemory<byte> memory, CancellationToken token = default);
    ValueTask SendAll(ReadOnlyMemory<byte> memory, CancellationToken token = default);
    ValueTask SendAll(ReadOnlySequence<byte> sequence, CancellationToken token = default);

    ValueTask Start(CancellationToken token = default);
}

public interface IZTransport<TSelf, in TEndpoint> : IZTransport
{
    public static abstract ValueTask<TSelf> Bind(IZSocket zsocket, TEndpoint endpoint,
        CancellationToken token = default);

    public static abstract ValueTask<TSelf> Connect(IZSocket zsocket, TEndpoint endpoint,
        CancellationToken token = default);
}

public class ZSocketTransport : IZTransport<ZSocketTransport, EndPoint>, IDisposable
{
    private readonly Socket socket;
    public required IZSocket ZSocket { get; init; }
    private bool disposed;

    /// <summary>
    ///
    /// </summary>
    /// <param name="socket"></param>
    /// <exception cref="ArgumentException">
    /// If `socket.SocketType` is not `Stream`
    /// </exception>
    public ZSocketTransport(Socket socket)
    {
        if (socket.SocketType != SocketType.Stream)
        {
            throw new ArgumentException($"{nameof(socket)} must be of type {SocketType.Stream} for ZeroMQ");
        }

        this.socket = socket;
    }

    public static ValueTask<ZSocketTransport> Bind(IZSocket zsocket, EndPoint endpoint,
        CancellationToken token = default)
    {
        if (token.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<ZSocketTransport>(token);
        }

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(endpoint);
        return ValueTask.FromResult(new ZSocketTransport(socket)
        {
            ZSocket = zsocket
        });
    }

    public static async ValueTask<ZSocketTransport> Connect(IZSocket zsocket, EndPoint endpoint,
        CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(endpoint, token);
        return new ZSocketTransport(socket)
        {
            ZSocket = zsocket
        };
    }

    public ValueTask<int> Send(ReadOnlyMemory<byte> memory, CancellationToken token = default)
    {
        return socket.SendAsync(memory, token);
    }

    public async ValueTask SendAll(ReadOnlyMemory<byte> memory, CancellationToken token = default)
    {
        var sent = 0;
        var length = memory.Length;
        while (true)
        {
            sent += await Send(memory[sent..], token);
            if (sent == length)
                break;
        }
    }

    public async ValueTask SendAll(ReadOnlySequence<byte> sequence, CancellationToken token = default)
    {
        foreach (var memory in sequence)
        {
            await SendAll(memory, token);
        }
    }

    public async ValueTask Start(CancellationToken token = default)
    {
        socket.Listen();
        while (!disposed)
        {
            var connection = await socket.AcceptAsync(token);
            _ = Handle(connection, token);
        }
    }

    private async Task Handle(Socket received, CancellationToken token)
    {
        while (!disposed)
        {
            using var owner = ZSocket.MemoryPool.Rent(1024);
            var count = await received.ReceiveAsync(owner.Memory, token);
        }
    }

    public void Dispose()
    {
        disposed = true;
        socket.Dispose();
    }
}
