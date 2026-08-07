using System.Buffers;
using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// High-level queue surface: wraps a callback socket type, takes over its
/// frame callback at construction, materializes messages per peer (0004), and
/// delivers them through a bounded Channel. The wrapped socket is never
/// exposed; connection and direct send forward to it.
/// </summary>
public sealed class ZQueueSocket<TSocket> : IZSocket
    where TSocket : ZSocketBase
{
    private readonly TSocket socket;
    private readonly MemoryPool<byte> pool;
    private readonly Channel<IZMessage> receiveChannel;
    private readonly Channel<IZMessage>? sendChannel;
    private readonly Task? sendPump;
    private readonly List<ZBufferRef> accumulator = [];
    private readonly List<Task> backgroundTasks = [];
    private readonly Lock stateLock = new();
    private readonly CancellationTokenSource cts = new();
    private int closed;

    internal ZQueueSocket(TSocket socket, ZQueueSocketOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        this.socket = socket;
        options ??= new ZQueueSocketOptions();
        pool = options.Pool ?? MemoryPool<byte>.Shared;

        receiveChannel = Channel.CreateBounded<IZMessage>(
            new BoundedChannelOptions(options.ReceiveCapacity) { SingleReader = true });

        if (options.SendCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<IZMessage>(new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(cts.Token);
        }

        socket.OnFrame += OnFrameHandler;
        socket.PeerEnded += OnPeerEnded;
    }

    public ChannelReader<IZMessage> Messages => receiveChannel.Reader;

    public ChannelWriter<IZMessage>? Outbound => sendChannel?.Writer;

    public ValueTask SendAsync(IZMessage message, CancellationToken token = default)
        => socket.SendAsync(message, token);

    public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
        => socket.SendAsync(bytes, token);

    public Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.BindAsync<TEndpoint, TTransport>(endpoint, token);

    public Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.ConnectAsync<TEndpoint, TTransport>(endpoint, token);

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.UnbindAsync<TEndpoint, TTransport>(endpoint, token);

    public Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.DisconnectAsync<TEndpoint, TTransport>(endpoint, token);

    public async Task CloseAsync(CancellationToken token = default)
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
        {
            return;
        }

        socket.OnFrame -= OnFrameHandler;
        socket.PeerEnded -= OnPeerEnded;
        await cts.CancelAsync();
        receiveChannel.Writer.TryComplete();
        sendChannel?.Writer.TryComplete();

        await socket.CloseAsync(token);

        if (sendPump is not null)
        {
            try
            {
                await sendPump;
            }
            catch (OperationCanceledException)
            {
            }
        }

        Task[] tasks;
        lock (stateLock)
        {
            tasks = [.. backgroundTasks];
        }

        if (tasks.Length > 0)
        {
            try
            {
                await Task.WhenAll(tasks).WaitAsync(token);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ZeroMqProtocolException)
            {
            }
        }

        cts.Dispose();
    }

    public async ValueTask DisposeAsync() => await CloseAsync(CancellationToken.None);

    private bool OnFrameHandler(ZFrame frame, CancellationToken token)
    {
        var owner = pool.Rent(frame.Memory.Length);
        frame.Memory.CopyTo(owner.Memory);
        accumulator.Add(new ZBufferRef(owner, owner.Memory[..frame.Memory.Length]));

        if (frame.More)
        {
            return true;
        }

        IZMessage message = accumulator.Count == 1
            ? new ZMessage(accumulator[0])
            : new ZMultiMessage([.. accumulator]);
        accumulator.Clear();

        if (receiveChannel.Writer.TryWrite(message))
        {
            return true;
        }

        message.Dispose();
        TrackBackground(ResumePausedAsync());
        return false;
    }

    private void OnPeerEnded(Exception? failure)
    {
        ReleasePartial();
        if (failure is not null)
        {
            receiveChannel.Writer.TryComplete(failure);
        }
    }

    private void ReleasePartial()
    {
        foreach (var frame in accumulator)
        {
            frame.Release();
        }

        accumulator.Clear();
    }

    private async Task ResumePausedAsync()
    {
        await receiveChannel.Writer.WaitToWriteAsync(cts.Token);
        socket.ResumePaused();
    }

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        await foreach (var message in channel.Reader.ReadAllAsync(token))
        {
            await socket.SendAsync(message, token);
        }
    }

    private void TrackBackground(Task task)
    {
        lock (stateLock)
        {
            backgroundTasks.Add(task);
        }
    }
}
