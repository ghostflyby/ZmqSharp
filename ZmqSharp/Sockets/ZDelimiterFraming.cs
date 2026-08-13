namespace ZmqSharp.Sockets;

/// <summary>
/// The REQ/REP family empty-delimiter wire format (0010): requests and
/// replies carry a leading empty frame so a REP can route replies back to
/// the originating peer. Shared by REQ and REP (the two cores previously
/// duplicated these bodies verbatim, 0015 section 1); frames move (0007 M3).
/// </summary>
internal static class ZDelimiterFraming
{
    /// <summary>Prepends the empty delimiter frame; the message's frames move.</summary>
    public static ZMessage Encode(ZMessage message)
    {
        var frames = new List<ZFrame>(message.Count + 1) { EmptyFrame };
        for (var i = 0; i < message.Count; i++) frames.Add(message[i]);

        return new ZMessage(new ZMultiMessage([.. frames]));
    }

    /// <summary>
    /// Strips the leading empty delimiter; a wire message without it is a
    /// protocol error. <paramref name="messageKind"/> names the direction in
    /// the error ("request" / "reply").
    /// </summary>
    public static ZMessage Decode(ZMessage message, string messageKind)
    {
        var count = message.Count;
        if (count < 2 || message[0].ToSequence().Length != 0)
        {
            message.Dispose();
            throw new ZeroMqProtocolException($"{messageKind} is missing the leading empty delimiter");
        }

        var frames = new List<ZFrame>(count - 1);
        for (var i = 1; i < count; i++) frames.Add(message[i]);

        return frames.Count == 1
            ? new ZMessage(new ZSingleMessage(frames[0]))
            : new ZMessage(new ZMultiMessage([.. frames]));
    }

    internal static readonly ZFrame EmptyFrame = new(new ZSegment(Array.Empty<byte>(), 0, 0));
}
