using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>Forwards one connection's parser output into the socket dispatcher.</summary>
internal sealed class SocketSink(ZSocketBase socket, ZConnection peer) : IZMessageSink
{
    public ZReceiveAction? Decide(in ZReceiveContext context) => null;

    public bool OnBorrowed(ZMessageView message, CancellationToken token) => socket.Deliver(peer, message);

    public bool OnOwned(ZMessage message, CancellationToken token)
    {
        // The socket always runs connections in Borrowed mode; this is defensive.
        message.Dispose();
        return true;
    }
}
