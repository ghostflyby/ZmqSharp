namespace ZmqSharp;

/// <summary>
/// Reports that the connection-level guard rejected a frame.
/// Propagates as the connection failure through the existing teardown path; no
/// wire ERROR is sent for a traffic-phase rejection (0008 D5).
/// </summary>
public sealed class ZReceiveRejectedException(ZReceiveRejection rejection)
    : Exception($"Receive rejected: {rejection.Reason}; limit: {rejection.Limit}, actual: {rejection.Actual}.")
{
    public ZReceiveRejection Rejection { get; } = rejection;
}
