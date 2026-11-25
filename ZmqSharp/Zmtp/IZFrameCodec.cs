namespace ZmqSharp.Zmtp;

/// <summary>
/// Per-connection frame transformation. Each direction is serialized and may
/// run concurrently with the other. Outputs live until the next same-direction
/// call; the caller finishes writing or consuming them before that call.
/// </summary>
public interface IZFrameCodec : IDisposable
{
    ZmtpFrameData Encode(ZmtpFrameData logicalFrame);

    /// <summary>
    /// Decodes one wire frame. The decoded frame must preserve the wire MORE
    /// flag: the receive pipeline uses it as the message boundary for the
    /// per-message limits and materializer reset.
    /// </summary>
    ZmtpFrameData Decode(ZmtpFrameData wireFrame);

    long GetMaximumEncodedLength(long logicalBodyLimit);
}
