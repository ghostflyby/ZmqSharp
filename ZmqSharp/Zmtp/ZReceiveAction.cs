namespace ZmqSharp.Zmtp;

/// <summary>Per-frame materialization decision.</summary>
public readonly struct ZReceiveAction
{
    public ZReceiveMode Mode { get; init; }

    public bool Contiguous { get; init; }
}
