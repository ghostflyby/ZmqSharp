namespace ZmqSharp.Zmtp;

/// <summary>
/// Receive options. Decide is the v2 application-level policy hook; v1 keeps it
/// null (defaults to Policy + ContiguousFrameLimit).
/// </summary>
public sealed class ZReceiveOptions
{
    public ZReceiveMode Policy { get; init; } = ZReceiveMode.Pooled;

    /// <summary>Contiguous frame limit; 0 forces segmented materialization for every frame.</summary>
    public int ContiguousFrameLimit { get; init; } = 85_000;

    public ZMessageDecider? Decide { get; init; }
}
