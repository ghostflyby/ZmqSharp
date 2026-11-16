namespace ZmqSharp.Zmtp;

/// <summary>
/// Allocates the final segments for a frame in materialization mode: the
/// receive policy's zero-copy hook, fed to a framing reader (final frames)
/// or a codec-decoding pump (decoded frames) by the socket runtime (0008;
/// publicized with the wire-protocol seam in 0033).
/// </summary>
public delegate ZFrame ZFrameAllocator(int frameLength, bool more);
