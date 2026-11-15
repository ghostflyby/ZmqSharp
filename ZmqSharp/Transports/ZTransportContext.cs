using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>
/// The composition context a transport factory receives (0034): the socket's
/// framing dialect, the memory pool backing framing state, and the command
/// bound active during establishment. A byte transport constructs its
/// connection with it; a native message transport already owns its units and
/// ignores the framing.
/// </summary>
public readonly struct ZTransportContext
{
    private readonly IZFraming? _framing;
    private readonly MemoryPool<byte>? _pool;
    private readonly long _maxCommandSize;

    public ZTransportContext(
        IZFraming framing, MemoryPool<byte> pool, long maxCommandSize)
    {
        ArgumentNullException.ThrowIfNull(framing);
        ArgumentNullException.ThrowIfNull(pool);
        _framing = framing;
        _pool = pool;
        _maxCommandSize = maxCommandSize;
    }

    /// <summary>The framing dialect byte connections adopt; defaults to ZMTP.</summary>
    public IZFraming Framing => _framing ?? ZmtpFraming.Instance;

    /// <summary>The pool backing framing scratch and owned copies.</summary>
    public MemoryPool<byte> Pool => _pool ?? MemoryPool<byte>.Shared;

    /// <summary>The establishment-phase frame/command bound.</summary>
    public long MaxCommandSize =>
        _maxCommandSize > 0 ? _maxCommandSize : ZmtpFraming.DefaultMaxCommandSize;

    /// <summary>The default context: ZMTP, the shared pool, the default bound.</summary>
    public static ZTransportContext Default => new();
}
