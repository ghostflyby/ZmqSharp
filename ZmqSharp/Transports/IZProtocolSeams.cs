namespace ZmqSharp.Transports;

/// <summary>Reason for a protocol-fault close on a transport with a close handshake.</summary>
internal enum ZCloseReason
{
    /// <summary>A framing, command, or authentication violation.</summary>
    ProtocolError,

    /// <summary>A frame, message, or command exceeded its configured size.</summary>
    MessageTooBig,
}

/// <summary>
/// A message connection with a transport-level close handshake (ZWS). The
/// session, the wire protocol, and the runtime's post-establishment
/// socket-type rejection report a fatal fault through this seam; transports
/// without a close handshake simply do not implement it. Other runtime
/// faults (for example post-delivery validation) retire without a close.
/// </summary>
internal interface IZCloseHandshake
{
    ValueTask CloseAsync(ZCloseReason reason, CancellationToken token = default);
}

/// <summary>
/// The frame count a connection expects in one establishment unit. ZWS uses
/// the extra frame to report a multipart identity through its own diagnostic;
/// other transports keep the historical unlimited handshake shape.
/// </summary>
internal interface IZHandshakeFrameLimit
{
    int HandshakeFrameLimit { get; }
}
