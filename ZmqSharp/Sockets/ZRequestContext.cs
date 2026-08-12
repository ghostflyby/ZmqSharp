using System.Collections;
using ZmqSharp.Transports;

namespace ZmqSharp;

/// <summary>
/// REP request value (0010 section 3, 0007 M2): the originating peer plus the
/// interpreted request message. It is the sole owner of the message - the
/// REP core disposes it after the request handler completes, so the context
/// is valid only during the handler call and must not be retained.
/// </summary>
public readonly struct ZRequestContext : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZMessage message;

    internal ZRequestContext(IZConnection peer, ZMessage message)
    {
        Peer = peer;
        this.message = message;
    }

    /// <summary>The peer the request arrived from; replies route back to it.</summary>
    public IZConnection Peer { get; }

    public int Count => message.Count;

    public ZFrame this[int index] => message[index];

    public ZMessage.Enumerator GetEnumerator()
    {
        return message.GetEnumerator();
    }

    IEnumerator<ZFrame> IEnumerable<ZFrame>.GetEnumerator()
    {
        return GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
        message.Dispose();
    }
}
