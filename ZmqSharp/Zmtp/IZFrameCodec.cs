namespace ZmqSharp.Zmtp;

/// <summary>
/// Per-connection frame transformation. Each direction is serialized and may
/// run concurrently with the other. Outputs live until the next same-direction
/// call; the caller finishes writing or consuming them before that call.
/// </summary>
public interface IZFrameCodec : IDisposable
{
    ZmtpFrameData Encode(ZmtpFrameData logicalFrame);
    ZmtpFrameData Decode(ZmtpFrameData wireFrame);
    long GetMaximumEncodedLength(long logicalBodyLimit);
}
