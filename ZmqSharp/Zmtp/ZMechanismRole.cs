namespace ZmqSharp.Zmtp;

/// <summary>
/// Connection role in the ZMTP handshake. The role comes from the connection
/// direction (0016 D3): an outbound ConnectAsync yields a Client session, an
/// accepted connection yields a Server session. The greeting's as-server bit
/// is written from the role and the peer's bit is never enforced, matching
/// libzmq/NetMQ behavior.
/// </summary>
public enum ZMechanismRole
{
    Client,
    Server
}
