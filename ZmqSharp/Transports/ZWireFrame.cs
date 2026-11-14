using System.Buffers;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Transports;

/// <summary>
/// The frame-layer unit (0033): a wire frame - the protocol frame flags
/// (more/command; the frame layer is protocol-related, not neutral) plus the
/// frame body as the library's own ownership-carrying <see cref="ZFrame"/>.
/// The frame layer's currency: frame connections read and write whole
/// <see cref="ZWireFrame"/>s; boundaries are the transport's native units
/// (or the framing's delimiters over bytes).
/// </summary>
public readonly struct ZWireFrame
{
    public ZWireFrame(ZmtpFrameFlags flags, ZFrame frame)
    {
        Validate(flags);
        Flags = flags;
        Frame = frame;
    }

    /// <summary>The protocol frame flags: exactly one of none/more/command.</summary>
    public ZmtpFrameFlags Flags { get; }

    /// <summary>
    /// The frame body. Ownership follows the currency rule: a read frame is
    /// the caller's to dispose; a written frame stays the sender's.
    /// </summary>
    public ZFrame Frame { get; }

    /// <summary>The frame body's byte length (ZFrame.Count is the segment count).</summary>
    internal long BodyLength => Frame.ToSequence().Length;

    internal static void Validate(ZmtpFrameFlags flags)
    {
        if ((flags & ~(ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) != 0 ||
            (flags & (ZmtpFrameFlags.More | ZmtpFrameFlags.Command)) == (ZmtpFrameFlags.More | ZmtpFrameFlags.Command))
            throw new ZeroMqProtocolException("invalid logical frame flags");
    }

    /// <summary>A frame copied into a pooled segment; dispose the owner when done.</summary>
    internal readonly struct Pooled(ZWireFrame wire, IDisposable owner)
    {
        public ZWireFrame Wire { get; } = wire;
        public IDisposable Owner { get; } = owner;
    }

    /// <summary>Copies a body into a pooled frame (cold paths: commands, greetings, codec output).</summary>
    internal static Pooled Copy(ZmtpFrameFlags flags, ReadOnlySpan<byte> body)
    {
        var owner = MemoryPool<byte>.Shared.Rent(body.Length == 0 ? 1 : body.Length);
        try
        {
            body.CopyTo(owner.Memory.Span);
            return Build(flags, owner, body.Length);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>Copies a multi-segment body into one pooled frame.</summary>
    internal static Pooled Copy(ZmtpFrameFlags flags, ReadOnlySequence<byte> body)
    {
        var length = checked((int)body.Length);
        var owner = MemoryPool<byte>.Shared.Rent(length == 0 ? 1 : length);
        try
        {
            body.CopyTo(owner.Memory.Span);
            return Build(flags, owner, length);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A frame's body as one contiguous memory, copying multi-segment frames
    /// (cold path); single-segment frames are returned without copy.
    /// </summary>
    internal static ReadOnlyMemory<byte> Contiguous(ZFrame frame)
    {
        var sequence = frame.ToSequence();
        if (sequence.IsSingleSegment) return sequence.First;

        var length = checked((int)sequence.Length);
        var buffer = GC.AllocateUninitializedArray<byte>(length);
        sequence.CopyTo(buffer);
        return buffer;
    }

    private static Pooled Build(ZmtpFrameFlags flags, IMemoryOwner<byte> owner, int length)
    {
        var more = (flags & ZmtpFrameFlags.More) != 0;
        return new Pooled(new ZWireFrame(flags, new ZFrame(new ZSegment(owner, 0, length), more)), owner);
    }
}
