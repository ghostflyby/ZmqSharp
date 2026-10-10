using Xunit;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
///     Unit tests for the receive policy (allocation only, 0008 D1/D6) and the
///     connection-level numeric limits enforced by the guard (0008 D2/D4), plus
///     the checked accumulation guard (0008 D3/D6).
/// </summary>
public sealed class ZReceiveOptionsTests
{
    [Fact]
    public void FrameLimit_AtLimitAccepts_OnePastRejects()
    {
        // Limits are enforced by the connection-level guard, not the policy.
        Assert.Null(ZReceiveGuard.CheckLimits(
            100,
            100,
            0,
            100,
            long.MaxValue,
            int.MaxValue));

        var rejection = ZReceiveGuard.CheckLimits(
            101,
            101,
            0,
            100,
            long.MaxValue,
            int.MaxValue);
        Assert.NotNull(rejection);
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, rejection.Value.Reason);
        Assert.Equal(100, rejection.Value.Limit);
        Assert.Equal(101, rejection.Value.Actual);
    }

    [Fact]
    public void MessageLimit_AtLimitAccepts_OnePastRejects()
    {
        Assert.Null(ZReceiveGuard.CheckLimits(
            1,
            100,
            0,
            long.MaxValue,
            100,
            int.MaxValue));

        var rejection = ZReceiveGuard.CheckLimits(
            1,
            101,
            0,
            long.MaxValue,
            100,
            int.MaxValue);
        Assert.NotNull(rejection);
        Assert.Equal(ZReceiveRejectionReason.MessageTooLarge, rejection.Value.Reason);
        Assert.Equal(100, rejection.Value.Limit);
        Assert.Equal(101, rejection.Value.Actual);
    }

    [Fact]
    public void FramesPerMessage_AtLimitAccepts_OnePastRejects()
    {
        Assert.Null(ZReceiveGuard.CheckLimits(
            1,
            3,
            2,
            long.MaxValue,
            long.MaxValue,
            3));

        var rejection = ZReceiveGuard.CheckLimits(
            1,
            4,
            3,
            long.MaxValue,
            long.MaxValue,
            3);
        Assert.NotNull(rejection);
        Assert.Equal(ZReceiveRejectionReason.TooManyFrames, rejection.Value.Reason);
        Assert.Equal(3, rejection.Value.Limit);
        Assert.Equal(4, rejection.Value.Actual);
    }

    [Fact]
    public void Limits_UnlimitedByDefault_AcceptEveryFrame()
    {
        Assert.Null(ZReceiveGuard.CheckLimits(
            int.MaxValue,
            long.MaxValue,
            int.MaxValue - 1, // the int.MaxValue-th frame is still in range
            long.MaxValue,
            long.MaxValue,
            int.MaxValue));
    }

    [Fact]
    public void Limits_EvaluateInFixedOrder_FirstViolationWins()
    {
        // Frame violation wins over the message total.
        var frameFirst = ZReceiveGuard.CheckLimits(
            11,
            11,
            0,
            10,
            20,
            2);
        Assert.NotNull(frameFirst);
        Assert.Equal(ZReceiveRejectionReason.FrameTooLarge, frameFirst.Value.Reason);

        // Message-total violation wins over the frame count.
        var messageFirst = ZReceiveGuard.CheckLimits(
            5,
            25,
            3,
            long.MaxValue,
            20,
            2);
        Assert.NotNull(messageFirst);
        Assert.Equal(ZReceiveRejectionReason.MessageTooLarge, messageFirst.Value.Reason);

        // Frame-count violation wins when the earlier limits are satisfied.
        var countFirst = ZReceiveGuard.CheckLimits(
            5,
            15,
            3,
            long.MaxValue,
            20,
            2);
        Assert.NotNull(countFirst);
        Assert.Equal(ZReceiveRejectionReason.TooManyFrames, countFirst.Value.Reason);
    }

    [Fact]
    public void Guard_Overflow_ReportsFailureInsteadOfThrowing()
    {
        Assert.False(ZReceiveGuard.TryAccumulate(long.MaxValue, 1, out _));
        Assert.True(ZReceiveGuard.TryAccumulate(long.MaxValue - 1, 1, out var total));
        Assert.Equal(long.MaxValue, total);
        Assert.True(ZReceiveGuard.TryAccumulate(41, 1, out var small));
        Assert.Equal(42, small);
    }

    [Fact]
    public void QueueOptions_DefaultPolicy_IsDefaultConfiguration()
    {
        var options = new ZSocketOptions();

        var policy = options.ReceivePolicy;
        var receiveOptions = Assert.IsType<ZReceiveOptions>(policy);
        Assert.Equal(ZReceiveMode.Pooled, receiveOptions.Mode);
        Assert.Equal(85_000, receiveOptions.ContiguousFrameLimit);

        // The policy is allocation-only; limits are socket-level and default
        // to effectively unlimited.
        Assert.Equal(long.MaxValue, options.MaxFrameLength);
        Assert.Equal(long.MaxValue, options.MaxMessageLength);
        Assert.Equal(int.MaxValue, options.MaxFramesPerMessage);

        // The default configuration accepts a small frame pooled and contiguous.
        var allocation = policy.Decide(new ZReceiveContext { FrameLength = 100 });
        Assert.Equal(ZReceiveMode.Pooled, allocation.Mode);
        Assert.False(allocation.Segmented);
    }
}
