using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZmqSharp.Messages;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>
/// High-level queue surface: wraps a callback socket type, takes over its
/// per-peer frame delivery at construction, materializes each peer's messages
/// into that peer's own bounded queue (0004), and exposes an aggregate reader
/// over the peer queues. The wrapped socket is never exposed.
/// </summary>
public sealed class ZQueueSocket<TSocket> : ZAsyncState, IZSocket
    where TSocket : ZSocketBase
{
    private const int SegmentBlockSize = 8192;

    private sealed class PeerState
    {
        public required Channel<IZMessage> Queue { get; init; }

        public List<ZFrameSegments> Accumulator { get; } = [];

        public int FrameIndex { get; set; }

        public long AccumulatedLength { get; set; }
    }

    /// <summary>Reads from every peer queue; the peer queues are the only physical queues.</summary>
    private sealed class AggregateReader(ZQueueSocket<TSocket> owner) : ChannelReader<IZMessage>
    {
        public override Task Completion => owner.completion.Task;

        public override bool TryRead([MaybeNullWhen(false)] out IZMessage item)
        {
            foreach (var state in owner.SnapshotPeers())
            {
                if (!state.Queue.Reader.TryRead(out var candidate)) continue;
                item = candidate;
                return true;
            }

            item = null;
            return false;
        }

        public override async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        {
            var completed = owner.completion.Task;
            if (completed.IsCompleted)
            {
                return false;
            }

            var wake = owner.GetWakeTask();
            var done = await Task.WhenAny(wake, completed).WaitAsync(cancellationToken);
            return done == wake;
        }
    }

    private readonly TSocket socket;
    private readonly Channel<IZMessage>? sendChannel;
    private readonly Task? sendPump;
    private readonly Dictionary<IZConnection, PeerState> peers = [];
    private readonly int receiveCapacity;
    private readonly IZReceivePolicy? receivePolicy;
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Action<IZConnection, Exception?>? peerEnded;
    private TaskCompletionSource wakeGate = CreateGate();

    internal ZQueueSocket(TSocket socket, ZQueueSocketOptions? options = null)
        : base(options?.Pool ?? MemoryPool<byte>.Shared)
    {
        ArgumentNullException.ThrowIfNull(socket);
        this.socket = socket;
        options ??= new ZQueueSocketOptions();
        receiveCapacity = options.ReceiveCapacity;
        receivePolicy = options.ReceivePolicy;
        Messages = new AggregateReader(this);

        if (options.SendCapacity is { } sendCapacity)
        {
            sendChannel = Channel.CreateBounded<IZMessage>(new BoundedChannelOptions(sendCapacity));
            sendPump = SendPumpAsync(Cts.Token);
        }

        socket.SetFrameSink(OnPeerConnected);
        socket.SetFrameAllocator(CreateAllocator);
        socket.PeerEnded += OnPeerEnded;
    }

    public ChannelReader<IZMessage> Messages { get; }

    public ChannelWriter<IZMessage>? Outbound => sendChannel?.Writer;

    /// <summary>Raised when a peer connection ends; null = clean EOF, otherwise the failure.</summary>
    public event Action<IZConnection, Exception?>? PeerEnded
    {
        add
        {
            lock (StateLock)
            {
                peerEnded += value;
            }
        }
        remove
        {
            lock (StateLock)
            {
                peerEnded -= value;
            }
        }
    }

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

    public Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.UnbindAsync<TEndpoint, TTransport>(endpoint);

    public Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>
        => socket.DisconnectAsync<TEndpoint, TTransport>(endpoint);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref Closed, 1) != 0)
        {
            return;
        }

        socket.PeerEnded -= OnPeerEnded;
        await Cts.CancelAsync();
        sendChannel?.Writer.TryComplete();

        await socket.DisposeAsync();

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

        completion.TrySetResult();
        await AwaitBackgroundAsync();
        Cts.Dispose();
    }

    private ZFrameHandler OnPeerConnected(IZConnection connection)
    {
        var state = new PeerState
        {
            Queue = Channel.CreateBounded<IZMessage>(
                new BoundedChannelOptions(receiveCapacity) { SingleReader = true }),
        };
        lock (StateLock)
        {
            peers.Add(connection, state);
        }

        TrackBackground(WakeOnReadableAsync(state));
        return (frame, ct) => OnPeerFrame(state, frame);
    }

    private bool OnPeerFrame(PeerState state, ZFrame frame)
    {
        var frameSegments = frame.Segments;
        if (frameSegments.Single is { Owner: var owner } && ReferenceEquals(owner, ZBufferRef.NoopOwner))
        {
            var poolOwner = Pool.Rent(frame.Memory.Length);
            frame.Memory.CopyTo(poolOwner.Memory);
            state.Accumulator.Add(new ZFrameSegments
            {
                Single = new ZBufferRef(poolOwner, poolOwner.Memory[..frame.Memory.Length]),
            });
        }
        else
        {
            state.Accumulator.Add(frameSegments);
        }

        if (frame.More)
        {
            return true;
        }

        IZMessage message = BuildMessage(state.Accumulator);
        state.Accumulator.Clear();
        state.FrameIndex = 0;
        state.AccumulatedLength = 0;

        if (state.Queue.Writer.TryWrite(message))
        {
            return true;
        }

        message.Dispose();
        TrackBackground(ResumePeerAsync(state));
        return false;
    }

    private static IZMessage BuildMessage(List<ZFrameSegments> frames)
    {
        if (frames.Count != 1)
        {
            return new ZMultiMessage([.. frames]);
        }

        var frame = frames[0];
        if (frame.Single is { } single)
        {
            return new ZMessage(single);
        }

        if (frame.Many is { } many)
        {
            return new ZMessage(many[0], many[1..]);
        }

        return new ZMultiMessage([]);
    }

    private ZFrameAllocator CreateAllocator(IZConnection connection)
    {
        return (length, more) =>
        {
            PeerState state;
            lock (StateLock)
            {
                if (!peers.TryGetValue(connection, out var s))
                {
                    throw new InvalidOperationException("frame allocator invoked for an unknown peer");
                }

                state = s;
            }

            var frameIndex = state.FrameIndex;
            var accumulated = state.AccumulatedLength + length;
            state.FrameIndex = frameIndex + 1;
            state.AccumulatedLength = accumulated;

            var allocation = DecideAllocation(frameIndex, accumulated, length, more);
            return AllocateSegments(allocation, length);
        };
    }

    private ZReceiveAllocation DecideAllocation(
        int frameIndex,
        long accumulatedLength,
        int frameLength,
        bool more)
    {
        if (receivePolicy is null)
        {
            return new ZReceiveAllocation { Mode = ZReceiveMode.Pooled };
        }

        var context = new ZReceiveContext
        {
            FrameLength = frameLength,
            HasMore = more,
            FrameIndex = frameIndex,
            AccumulatedLength = accumulatedLength,
        };
        return receivePolicy.Decide(context);
    }

    private ZBufferRef Allocate(ZReceiveMode mode, int length)
    {
        if (mode == ZReceiveMode.Owned)
        {
            var buffer = GC.AllocateUninitializedArray<byte>(length);
            return new ZBufferRef(buffer, buffer);
        }

        var owner = Pool.Rent(length);
        return new ZBufferRef(owner, owner.Memory[..length]);
    }

    private ZFrameSegments AllocateSegments(
        ZReceiveAllocation allocation,
        int length)
    {
        if (allocation.Segmented && length > SegmentBlockSize)
        {
            var count = (length + SegmentBlockSize - 1) / SegmentBlockSize;
            var segments = new ZBufferRef[count];
            var offset = 0;
            for (var i = 0; i < count; i++)
            {
                var blockLength = Math.Min(SegmentBlockSize, length - offset);
                segments[i] = Allocate(allocation.Mode, blockLength);
                offset += blockLength;
            }

            return new ZFrameSegments { Many = segments };
        }

        return new ZFrameSegments
        {
            Single = Allocate(allocation.Mode, length),
        };
    }

    private async Task ResumePeerAsync(PeerState state)
    {
        await state.Queue.Writer.WaitToWriteAsync(Cts.Token);
        socket.ResumePaused();
    }

    private async Task WakeOnReadableAsync(PeerState state)
    {
        while (await state.Queue.Reader.WaitToReadAsync(Cts.Token))
        {
            Wake();
        }
    }

    private void OnPeerEnded(IZConnection connection, Exception? failure)
    {
        PeerState? state;
        lock (StateLock)
        {
            if (!peers.Remove(connection, out state))
            {
                return;
            }
        }

        foreach (var frame in state.Accumulator)
        {
            if (frame.Single is { } single)
            {
                single.Release();
            }
            else if (frame.Many is { } many)
            {
                foreach (var segment in many)
                {
                    segment.Release();
                }
            }
        }

        state.Accumulator.Clear();
        state.Queue.Writer.TryComplete();

        Action<IZConnection, Exception?>? handler;
        lock (StateLock)
        {
            handler = peerEnded;
        }

        handler?.Invoke(connection, failure);
    }

    private Task GetWakeTask()
    {
        lock (StateLock)
        {
            return wakeGate.Task;
        }
    }

    private void Wake()
    {
        lock (StateLock)
        {
            wakeGate.TrySetResult();
            wakeGate = CreateGate();
        }
    }

    private List<PeerState> SnapshotPeers()
    {
        lock (StateLock)
        {
            return [.. peers.Values];
        }
    }

    private static TaskCompletionSource CreateGate()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task SendPumpAsync(CancellationToken token)
    {
        var channel = sendChannel ?? throw new InvalidOperationException("send channel is not configured");
        await foreach (var message in channel.Reader.ReadAllAsync(token))
        {
            await socket.SendAsync(message, token);
        }
    }
}
