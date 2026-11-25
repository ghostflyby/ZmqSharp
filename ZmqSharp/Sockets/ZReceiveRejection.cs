namespace ZmqSharp;

/// <summary>The rejection payload of the connection-level guard (0008 D1).</summary>
public readonly struct ZReceiveRejection
{
    /// <summary>Classification of the rejection.</summary>
    public ZReceiveRejectionReason Reason { get; init; }

    /// <summary>The configured limit, when the rejection is a numeric-limit violation.</summary>
    public long? Limit { get; init; }

    /// <summary>
    /// The observed value, when the rejection is a numeric-limit violation.
    /// A streaming transport may report the bytes observed so far rather than
    /// the final size (the value is then a lower bound on the offending size).
    /// </summary>
    public long? Actual { get; init; }
}
