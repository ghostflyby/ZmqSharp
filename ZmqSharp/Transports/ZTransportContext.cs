using System.Buffers;

namespace ZmqSharp.Transports;

/// <summary>
/// The composition context a transport factory receives (0034): the socket's
/// framing dialect and the memory pool backing framing state. A byte
/// transport constructs its connection with it; a native message transport
/// already owns its units and ignores the framing. Receive limits are not
/// part of it: the runtime configures the connection after establishment.
/// </summary>
public readonly struct ZTransportContext
{
    private readonly IZFraming? _framing;
    private readonly MemoryPool<byte>? _pool;

    public ZTransportContext(IZFraming framing, MemoryPool<byte> pool)
    {
        ArgumentNullException.ThrowIfNull(framing);
        ArgumentNullException.ThrowIfNull(pool);
        _framing = framing;
        _pool = pool;
    }

    /// <summary>The framing dialect byte connections adopt; defaults to ZMTP.</summary>
    public IZFraming Framing => _framing ?? ZmtpFraming.Instance;

    /// <summary>The pool backing framing scratch and owned copies.</summary>
    public MemoryPool<byte> Pool => _pool ?? MemoryPool<byte>.Shared;

    /// <summary>The default context: ZMTP and the shared pool.</summary>
    public static ZTransportContext Default => new();
}
