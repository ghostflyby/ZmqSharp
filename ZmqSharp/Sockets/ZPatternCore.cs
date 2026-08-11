using System.Threading;
using ZmqSharp.Messages;
using ZmqSharp.Transports;

namespace ZmqSharp.Sockets;

/// <summary>
/// Per-pattern wire semantics, composed with the transport core rather than
/// subclassed (0007 section 2.2): outbound selection and the advertised
/// Socket-Type. The transport core owns transport mechanics only; a socket
/// type is a thin composition root binding one core to the base.
/// </summary>
internal interface IPatternCore
{
    /// <summary>Selects the outbound connection for a message; null = drop.</summary>
    IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers);

    /// <summary>ZMTP Socket-Type metadata advertised in the READY handshake.</summary>
    string SocketTypeName { get; }
}

/// <summary>PAIR semantics: single peer, no routing.</summary>
internal sealed class ZPairCore : IPatternCore
{
    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
        => peers.IsEmpty ? null : peers[0];

    public string SocketTypeName => "PAIR";
}

/// <summary>DEALER semantics: fair dispatch, round-robin outbound.</summary>
internal sealed class ZDealerCore : IPatternCore
{
    private int next;

    public IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers)
    {
        if (peers.IsEmpty)
        {
            return null;
        }

        int index = (Interlocked.Increment(ref next) - 1) % peers.Length;
        return peers[index];
    }

    public string SocketTypeName => "DEALER";
}
