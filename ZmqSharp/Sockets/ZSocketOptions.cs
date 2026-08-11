using System.Buffers;

namespace ZmqSharp.Sockets;

/// <summary>Socket configuration (transport core).</summary>
public sealed class ZSocketOptions
{
    /// <summary>
    /// Lowest configurable command-size limit: prevents disabling the limit
    /// entirely (0008 Slice B completion gate).
    /// </summary>
    public const int MinMaxCommandSize = 256;

    private int maxCommandSize = Zmtp.ZmtpParser.DefaultMaxCommandSize;
    private int handshakeTimeoutMs = 30_000;
    private int maxIncompleteHandshakes = 1024;

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
        get => maxCommandSize;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, MinMaxCommandSize);
            maxCommandSize = value;
        }
    }

    /// <summary>
    /// ZMTP handshake timeout in milliseconds (0006 3.2); a peer that does not
    /// complete the greeting/READY exchange within this window faults its
    /// establishment. Default 30 s. Zero disables the timeout.
    /// </summary>
    public int HandshakeTimeoutMs
    {
        get => handshakeTimeoutMs;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            handshakeTimeoutMs = value;
        }
    }

    /// <summary>
    /// Maximum concurrently incomplete handshakes on the inbound (accepted)
    /// surface (0006 3.2). A slow-connecting flood beyond this is dropped with
    /// cancellation. Default 1024; zero disables the limit. Outbound
    /// ConnectAsync is caller-initiated and not gated.
    /// </summary>
    public int MaxIncompleteHandshakes
    {
        get => maxIncompleteHandshakes;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            maxIncompleteHandshakes = value;
        }
    }
}
