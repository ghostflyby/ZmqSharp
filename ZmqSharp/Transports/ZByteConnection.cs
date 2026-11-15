using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The shared message-first base for byte connections (0034). A byte
/// transport implements only the byte primitives; this base owns the framing
/// view that turns them into whole messages in the connection's dialect and
/// forwards the established receive policy. The view is non-owning, so
/// disposing or aborting the connection never recurses through the view.
/// </summary>
public abstract class ZByteConnection : IZConnection
{
    private readonly ZTransportContext _context;
    private IZMessageConnection? _messages;

    protected ZByteConnection() : this(ZTransportContext.Default)
    {
    }

    protected ZByteConnection(in ZTransportContext context)
    {
        _context = context;
    }

    /// <summary>The message view backing the inherited message surface.</summary>
    internal IZMessageConnection Messages => _messages ??= CreateView();

    private IZMessageConnection CreateView()
    {
        // A standalone connection is unbounded: limits are composition
        // policy. The runtime sets the establishment command bound before the
        // handshake and the traffic composition after it; the dialect and pool
        // come from the transport context.
        var viewContext = new ZTransportContext(_context.Framing, _context.Pool, long.MaxValue);
        return viewContext.Framing.Adapt(this, viewContext, ownsConnection: false);
    }

    public abstract ValueTask<int> ReadAsync(
        Memory<byte> destination, CancellationToken token = default);

    public abstract ValueTask WriteAsync(
        ReadOnlyMemory<byte> bytes, CancellationToken token = default);

    public abstract ValueTask WriteAsync(
        ReadOnlySequence<byte> bytes, CancellationToken token = default);

    /// <summary>Idempotently aborts the underlying I/O.</summary>
    public abstract void Abort();

    public ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default)
        => Messages.ReadUnitAsync(token);

    public ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default)
        => Messages.WriteMessageAsync(message, token);

    public ValueTask WriteCommandAsync(
        ReadOnlyMemory<byte> body, CancellationToken token = default)
        => Messages.WriteCommandAsync(body, token);

    /// <summary>Byte-backed views copy the wire during the call (0033).</summary>
    public bool RetainsMessages => false;

    /// <summary>The byte side is this connection itself.</summary>
    public IZConnection ByteOrigin => this;

    public void ConfigureReceive(in ZReceiveComposition composition)
        => Messages.ConfigureReceive(composition);

    /// <summary>Releases the message view's state, then the byte resource.</summary>
    public void Dispose()
    {
        _messages?.Dispose();
        _messages = null;
        DisposeCore();
    }

    /// <summary>Releases the byte resource; the message view is already released.</summary>
    protected virtual void DisposeCore()
    {
    }
}

/// <summary>
/// The internal fast path a framing may implement to stream one already
/// encoded frame as a single logical write (the codec session's hot path).
/// </summary>
internal interface IZStagedFrameWriter
{
    ValueTask WriteStagedFrameAsync(
        ZmtpFrameFlags flags, ZFrame frame, CancellationToken token);
}

/// <summary>
/// Resolves the staged-frame capability through either the connection itself
/// (a native message connection or a raw framing view) or the framing view
/// inside a <see cref="ZByteConnection"/>.
/// </summary>
internal static class ZStagedWrites
{
    public static bool TryGet(
        IZMessageConnection connection, [NotNullWhen(true)] out IZStagedFrameWriter? writer)
    {
        if (connection is IZStagedFrameWriter direct)
        {
            writer = direct;
            return true;
        }

        if (connection is ZByteConnection byteConnection
            && byteConnection.Messages is IZStagedFrameWriter view)
        {
            writer = view;
            return true;
        }

        writer = null;
        return false;
    }
}
