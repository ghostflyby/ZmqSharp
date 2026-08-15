using System.Buffers;
using System.Threading.Channels;
using ZmqSharp.Zmtp;

namespace ZmqSharp;

/// <summary>Socket configuration (transport core).</summary>
public sealed class ZSocketOptions
{
    /// <summary>
    /// Lowest configurable command-size limit: prevents disabling the limit
    /// entirely (0008 Slice B completion gate).
    /// </summary>
    public const int MinMaxCommandSize = 256;

    /// <summary>
    /// Memory pool used for the send copy path; defaults to the shared pool.
    /// <c>MemoryPool&lt;byte&gt;.Shared</c>'s Dispose is a no-op, which makes it a
    /// safe default; the library never disposes an injected pool regardless,
    /// ownership stays with the caller.
    /// </summary>
    public MemoryPool<byte> Pool { get; init; } = MemoryPool<byte>.Shared;

    /// <summary>
    /// Maximum accepted ZMTP command-frame size; a larger command rejects the
    /// connection (0006 3.2, 0008 Slice B). Defaults to 1 MiB and cannot be
    /// lowered below <see cref="MinMaxCommandSize"/>.
    /// </summary>
    public int MaxCommandSize
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, MinMaxCommandSize);
            field = value;
        }
    } = ZmtpParser.DefaultMaxCommandSize;

    /// <summary>
    /// ZMTP handshake timeout in milliseconds (0006 3.2); a peer that does not
    /// complete the greeting/READY exchange within this window faults its
    /// establishment. Default 30 s. Zero disables the timeout.
    /// </summary>
    public int HandshakeTimeoutMs
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            field = value;
        }
    } = 30_000;

    /// <summary>
    /// ZMTP security mechanism configuration (0016 section 7). Defaults to the
    /// NULL mechanism, preserving the current behavior; only one mechanism can
    /// be configured per socket (ZMTP allows one mechanism per connection).
    /// The mechanism is resolved once at socket construction, so this is safe
    /// under Native AOT.
    /// </summary>
    public ZSecurityOptions Security { get; init; } = ZSecurityOptions.Null;

    /// <summary>
    /// Maximum concurrently incomplete handshakes on the inbound (accepted)
    /// surface (0006 3.2). A slow-connecting flood beyond this is dropped with
    /// cancellation. Default 1024; zero disables the limit. Outbound
    /// ConnectAsync is caller-initiated and not gated.
    /// </summary>
    public int MaxIncompleteHandshakes
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            field = value;
        }
    } = 1024;

    /// <summary>
    /// The receive surface composed at construction (0023): <see
    /// cref="ZReceiveSurface.Queue"/> (default) makes the socket its own
    /// queue surface - per-peer bounded queues read through
    /// <see cref="ZQueueSocketBase.Messages"/> - and <see
    /// cref="ZReceiveSurface.Callback"/> opts out to the raw
    /// <c>OnFrame</c> surface. Only consulted when <see cref="MessageSink"/>
    /// is null: a custom sink implies callback semantics. Ignored by REQ and
    /// REP, whose protocol cores consume inbound messages regardless of
    /// surface.
    /// </summary>
    public ZReceiveSurface ReceiveSurface { get; init; } = ZReceiveSurface.Queue;

    /// <summary>
    /// Custom delivery sink, bound at construction (0023): complete messages
    /// are delivered to the sink, per peer and serialized, and the socket
    /// composes no queue surface - <see cref="ReceiveSurface"/> is not
    /// consulted and <see cref="ZQueueSocketBase.Messages"/> is unavailable.
    /// Defaults to null, in which case <see cref="ReceiveSurface"/> decides
    /// between the queue surface and the raw <c>OnFrame</c> surface.
    /// </summary>
    public IPatternSink? MessageSink { get; init; }

    /// <summary>
    /// Per-peer receive queue factory (0009); the library forces
    /// SingleReader and the factory's SingleWriter per connection. Defaults
    /// to a bounded SPSC queue with capacity 16. BCL channel options convert
    /// implicitly into a factory, so <c>new BoundedChannelOptions(16)</c> is
    /// assignable here. Must not be set on the callback surface or with a
    /// custom <see cref="MessageSink"/> - the queue is never composed there
    /// (throws at construction).
    /// </summary>
    public ZQueueFactory ReceiveQueueFactory
    {
        get => receiveQueueFactory ?? new BoundedChannelOptions(16) { SingleWriter = true };
        init => receiveQueueFactory = value;
    }

    /// <summary>
    /// When set, enables the optional outbound channel built by this factory
    /// (0009) on the queue surface. Must not be set on the callback surface
    /// or with a custom <see cref="MessageSink"/>.
    /// </summary>
    public ZQueueFactory? SendQueueFactory { get; init; }

    /// <summary>
    /// Receive materialization policy; defaults to the numeric
    /// <see cref="ZReceiveOptions"/> configuration, which accepts every frame
    /// pooled, contiguous up to <c>ContiguousFrameLimit</c> and segmented
    /// above it. The policy only decides allocation; the rejection limits
    /// below are enforced outside it. Must not be set on the callback surface
    /// or with a custom <see cref="MessageSink"/>.
    /// </summary>
    public IZReceivePolicy ReceivePolicy
    {
        get => receivePolicy ?? new ZReceiveOptions();
        init => receivePolicy = value;
    }

    /// <summary>
    /// Maximum accepted frame length; a longer frame rejects the connection
    /// (0008 D3/D6). Defaults to effectively unlimited. Must not be set on
    /// the callback surface or with a custom <see cref="MessageSink"/>.
    /// </summary>
    public long MaxFrameLength
    {
        get => maxFrameLength ?? long.MaxValue;
        init => maxFrameLength = value;
    }

    /// <summary>
    /// Maximum accepted accumulated message length; a larger total rejects
    /// the connection (0008 D3/D6). Defaults to effectively unlimited. Must
    /// not be set on the callback surface or with a custom
    /// <see cref="MessageSink"/>.
    /// </summary>
    public long MaxMessageLength
    {
        get => maxMessageLength ?? long.MaxValue;
        init => maxMessageLength = value;
    }

    /// <summary>
    /// Maximum accepted frames per message; more frames reject the connection
    /// (0008 D3/D6). Defaults to effectively unlimited. Must not be set on
    /// the callback surface or with a custom <see cref="MessageSink"/>.
    /// </summary>
    public int MaxFramesPerMessage
    {
        get => maxFramesPerMessage ?? int.MaxValue;
        init => maxFramesPerMessage = value;
    }

    // Nullable backing fields: a null means "not configured", so a socket
    // that never composes the queue surface can detect explicit queue
    // configuration and reject it at construction instead of silently
    // ignoring it (0023). The public getters stay non-null and declarative
    // (0008 D2). ReceivePolicy caches its lazily-created default.
    private ZQueueFactory? receiveQueueFactory;
    private IZReceivePolicy? receivePolicy;
    private long? maxFrameLength;
    private long? maxMessageLength;
    private int? maxFramesPerMessage;

    /// <summary>
    /// True when any queue-surface option was explicitly set (0023). A socket
    /// that composes no queue - a custom <see cref="MessageSink"/>, the
    /// callback surface, or REQ/REP - throws at construction when this is
    /// set, so silently-ignored configuration fails loudly.
    /// </summary>
    internal bool HasQueueConfiguration =>
        receiveQueueFactory is not null
        || SendQueueFactory is not null
        || receivePolicy is not null
        || maxFrameLength is not null
        || maxMessageLength is not null
        || maxFramesPerMessage is not null;
}
