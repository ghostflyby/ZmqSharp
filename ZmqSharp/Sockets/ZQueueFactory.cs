using System.Threading.Channels;

namespace ZmqSharp;

/// <summary>
/// Declaration-style channel construction strategy (0009), mirroring the
/// receive policy system. Every factory produces a
/// <c>Channel&lt;ZMessage&gt;</c>. Factories must be stateless and
/// thread-safe; the same factory may create many channels.
/// </summary>
public interface IZQueueFactory
{
    /// <summary>
    /// Creates a fresh channel. <paramref name="itemDropped"/> is the
    /// library's mandatory reclamation hook (0006 section 2.2): a bounded
    /// factory wires it into <c>CreateBounded</c> so a drop-mode discard is
    /// disposed; an unbounded factory has no drop concept and ignores it
    /// (explicit drains still reclaim). <c>SingleReader</c> is always forced
    /// on: the library is the sole reader of every channel it builds.
    /// </summary>
    Channel<ZMessage> Create(Action<ZMessage> itemDropped);
}

/// <summary>
/// Message-channel factory as a socket configuration value. The concrete
/// factories are internal; the public surface is this abstract base plus its
/// implicit conversions, so <c>ReceiveQueueFactory = new BoundedChannelOptions(16)</c>
/// works (the conversion operator lives here, since C# requires it on either
/// the source or the target type).
/// </summary>
public abstract class ZQueueFactory : IZQueueFactory
{
    /// <summary>Creates a fresh channel; see <see cref="IZQueueFactory.Create"/>.</summary>
    public abstract Channel<ZMessage> Create(Action<ZMessage> itemDropped);

    public static implicit operator ZQueueFactory(BoundedChannelOptions options)
    {
        return new ZBoundedQueueFactory(options);
    }

    public static implicit operator ZQueueFactory(UnboundedChannelOptions options)
    {
        return new ZUnboundedQueueFactory(options);
    }
}

/// <summary>
/// Bounded channel factory (configured by <see cref="BoundedChannelOptions"/>):
/// capacity is the HWM, so a full queue drops or backpressure per the
/// configured mode. Constructed from <see cref="BoundedChannelOptions"/>,
/// which is copied at construction and fixed: later mutation of the original
/// options instance does not affect this factory.
/// </summary>
internal sealed class ZBoundedQueueFactory : ZQueueFactory, IZQueueFactory
{
    public ZBoundedQueueFactory(BoundedChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = Build(options);
    }

    /// <summary>The fixed options snapshot applied to every created channel.</summary>
    internal BoundedChannelOptions Options { get; }

    /// <inheritdoc />
    public override Channel<ZMessage> Create(Action<ZMessage> itemDropped)
    {
        return Channel.CreateBounded(Options, itemDropped);
    }

    /// <summary>
    /// Copies the caller's options into the factory-owned snapshot, forcing
    /// <c>SingleReader</c> on (the library is the sole reader). The caller's
    /// <c>SingleWriter</c> choice is preserved: the receive side uses SPSC,
    /// while the outbound channel is a shared producer surface.
    /// </summary>
    private static BoundedChannelOptions Build(BoundedChannelOptions source)
    {
        return new BoundedChannelOptions(source.Capacity)
        {
            SingleReader = true,
            SingleWriter = source.SingleWriter,
            FullMode = source.FullMode,
            AllowSynchronousContinuations = source.AllowSynchronousContinuations
        };
    }
}

/// <summary>
/// Unbounded channel factory (configured by
/// <see cref="UnboundedChannelOptions"/>): no HWM, no full mode, never
/// blocks a writer, and no drop concept. Opt-in only: an unbounded receive
/// queue gives up the per-peer peak-memory bound (0004 constraint 3, revised
/// by 0009).
/// </summary>
internal sealed class ZUnboundedQueueFactory : ZQueueFactory, IZQueueFactory
{
    public ZUnboundedQueueFactory(UnboundedChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = Build(options);
    }

    /// <summary>The fixed options snapshot applied to every created channel.</summary>
    private UnboundedChannelOptions Options { get; }

    /// <inheritdoc />
    public override Channel<ZMessage> Create(Action<ZMessage> itemDropped)
    {
        return Channel.CreateUnbounded<ZMessage>(Options);
    }

    private static UnboundedChannelOptions Build(UnboundedChannelOptions source)
    {
        return new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = source.SingleWriter,
            AllowSynchronousContinuations = source.AllowSynchronousContinuations
        };
    }
}
