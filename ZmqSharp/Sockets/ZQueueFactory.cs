using System.Threading.Channels;
using ZmqSharp.Messages;

namespace ZmqSharp.Sockets;

/// <summary>
/// Declaration-style channel construction strategy (0009), mirroring the
/// receive policy system. The type parameter names the channel-option type
/// the factory is configured by (<see cref="BoundedChannelOptions"/> or
/// <see cref="UnboundedChannelOptions"/>); every factory produces a
/// <c>Channel&lt;ZMessage&gt;</c>. Factories must be stateless and
/// thread-safe; the same factory may create many channels.
/// </summary>
public interface IZQueueFactory<TOptions>
    where TOptions : ChannelOptions
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
/// Message-channel factory as a socket configuration value. Implements the
/// generic strategy contract closed at <see cref="ChannelOptions"/>, the
/// common option base, so any factory is consumable as an
/// <see cref="IZQueueFactory{TOptions}"/>; each concrete factory additionally
/// implements the contract closed at its own option type. BCL channel options
/// convert implicitly into a factory (the conversion operator lives on this
/// type, since C# requires it on either the source or the target type), so
/// <c>ReceiveQueueFactory = new BoundedChannelOptions(16)</c> works.
/// </summary>
public abstract class ZQueueFactory : IZQueueFactory<ChannelOptions>
{
    /// <summary>Creates a fresh channel; see <see cref="IZQueueFactory{TOptions}.Create"/>.</summary>
    public abstract Channel<ZMessage> Create(Action<ZMessage> itemDropped);

    public static implicit operator ZQueueFactory(BoundedChannelOptions options) => new ZBoundedQueueFactory(options);

    public static implicit operator ZQueueFactory(UnboundedChannelOptions options) => new ZUnboundedQueueFactory(options);
}

/// <summary>
/// Bounded channel factory (configured by <see cref="BoundedChannelOptions"/>):
/// capacity is the HWM, so a full queue drops or backpressures per the
/// configured mode. Constructed from <see cref="BoundedChannelOptions"/> (or
/// the convenience arguments), which are copied at construction and fixed:
/// later mutation of the original options instance does not affect this
/// factory.
/// </summary>
public sealed class ZBoundedQueueFactory : ZQueueFactory, IZQueueFactory<BoundedChannelOptions>
{
    public ZBoundedQueueFactory(
        int capacity,
        BoundedChannelFullMode fullMode = BoundedChannelFullMode.Wait,
        bool singleWriter = true,
        bool allowSynchronousContinuations = false)
    {
        Options = Build(new BoundedChannelOptions(capacity)
        {
            FullMode = fullMode,
            SingleWriter = singleWriter,
            AllowSynchronousContinuations = allowSynchronousContinuations,
        });
    }

    public ZBoundedQueueFactory(BoundedChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = Build(options);
    }

    /// <summary>The fixed options snapshot applied to every created channel.</summary>
    internal BoundedChannelOptions Options { get; }

    /// <inheritdoc />
    public override Channel<ZMessage> Create(Action<ZMessage> itemDropped)
        => Channel.CreateBounded(Options, itemDropped);

    /// <summary>
    /// Copies the caller's options into the factory-owned snapshot, forcing
    /// <c>SingleReader</c> on (the library is the sole reader). The caller's
    /// <c>SingleWriter</c> choice is preserved: the receive side uses SPSC,
    /// while the outbound channel is a shared producer surface.
    /// </summary>
    private static BoundedChannelOptions Build(BoundedChannelOptions source)
        => new(source.Capacity)
        {
            SingleReader = true,
            SingleWriter = source.SingleWriter,
            FullMode = source.FullMode,
            AllowSynchronousContinuations = source.AllowSynchronousContinuations,
        };
}

/// <summary>
/// Unbounded channel factory (configured by
/// <see cref="UnboundedChannelOptions"/>): no HWM, no full mode, never
/// blocks a writer, and no drop concept. Opt-in only: an unbounded receive
/// queue gives up the per-peer peak-memory bound (0004 constraint 3, revised
/// by 0009).
/// </summary>
public sealed class ZUnboundedQueueFactory : ZQueueFactory, IZQueueFactory<UnboundedChannelOptions>
{
    private readonly UnboundedChannelOptions copy;

    public ZUnboundedQueueFactory(bool singleWriter = true, bool allowSynchronousContinuations = false)
    {
        copy = Build(new UnboundedChannelOptions
        {
            SingleWriter = singleWriter,
            AllowSynchronousContinuations = allowSynchronousContinuations,
        });
    }

    public ZUnboundedQueueFactory(UnboundedChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        copy = Build(options);
    }

    /// <summary>The fixed options snapshot applied to every created channel.</summary>
    internal UnboundedChannelOptions Options => copy;

    /// <inheritdoc />
    public override Channel<ZMessage> Create(Action<ZMessage> itemDropped)
        => Channel.CreateUnbounded<ZMessage>(copy);

    private static UnboundedChannelOptions Build(UnboundedChannelOptions source)
        => new()
        {
            SingleReader = true,
            SingleWriter = source.SingleWriter,
            AllowSynchronousContinuations = source.AllowSynchronousContinuations,
        };
}
