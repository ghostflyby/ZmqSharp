using ZmqSharp.Messages;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Sockets;

/// <summary>Streams a connection's frames into the socket dispatcher and assembles owned messages.</summary>
internal sealed class SocketSink(ZSocketBase socket, ZConnection peer) : IZMessageSink
{
    private readonly List<ZBufferRef> frames = [];

    public bool OnFrame(ZFrame frame, CancellationToken token)
        => socket.DeliverFrame(peer, frame, frames);

    public void OnConnectionEnded()
    {
        foreach (var frame in frames)
        {
            frame.Release();
        }

        frames.Clear();
    }
}
