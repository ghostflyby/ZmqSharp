using System.Net;
using FluentAssertions;
using NetMQ;

namespace ZmqSharp.Tests;

/// <summary>Shared helpers for the NetMQ libzmq-compatible interop suite (0006 section 5).</summary>
internal static class InteropHelpers
{
    /// <summary>Test category trait marking the NetMQ interop suite.</summary>
    public const string InteropCategory = "Interop";

    public static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>Blocks until one frame is received (bounded by timeout).</summary>
    public static byte[] ReceiveFrame(NetMQSocket socket, TimeSpan timeout)
    {
        socket.TryReceiveFrameBytes(timeout, out var frame).Should().BeTrue("expected a frame within the timeout");
        return frame;
    }
}
