using System.Buffers;

namespace ZmqSharp.Messages;

/// <summary>
/// Borrowed message view: valid only during the low-level callback, zero
/// allocation, never Disposed. The parser reuses its backing buffers after the
/// callback returns, so references must not be kept.
/// </summary>
public readonly struct ZMessageView : IZMessage
{
    private readonly ZMessageData? data;

    internal ZMessageView(ZMessageData data) => this.data = data;

    public int FrameCount => data?.FrameCount ?? 0;

    public ReadOnlySequence<byte> GetFrame(int index)
    {
        if (data is null || index < 0 || index >= data.FrameCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        return data.GetFrame(index);
    }

    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory)
    {
        if (data is null || index < 0 || index >= data.FrameCount)
        {
            memory = default;
            return false;
        }

        return data.TryGetContiguousFrame(index, out memory);
    }

    public ReadOnlySequence<byte> Whole => data?.Whole ?? ReadOnlySequence<byte>.Empty;
}
