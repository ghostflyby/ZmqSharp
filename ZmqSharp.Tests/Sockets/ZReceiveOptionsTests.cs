using FluentAssertions;
using Xunit;

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
        ZReceiveGuard.CheckLimits(
            100,
            100,
            0,
            100,
            long.MaxValue,
            int.MaxValue).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            101,
            101,
            0,
            100,
            long.MaxValue,
            int.MaxValue);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        rejection.Value.Limit.Should().Be(100);
        rejection.Value.Actual.Should().Be(101);
    }

    [Fact]
    public void MessageLimit_AtLimitAccepts_OnePastRejects()
    {
        ZReceiveGuard.CheckLimits(
            1,
            100,
            0,
            long.MaxValue,
            100,
            int.MaxValue).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            1,
            101,
            0,
            long.MaxValue,
            100,
            int.MaxValue);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);
        rejection.Value.Limit.Should().Be(100);
        rejection.Value.Actual.Should().Be(101);
    }

    [Fact]
    public void FramesPerMessage_AtLimitAccepts_OnePastRejects()
    {
        ZReceiveGuard.CheckLimits(
            1,
            3,
            2,
            long.MaxValue,
            long.MaxValue,
            3).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            1,
            4,
            3,
            long.MaxValue,
            long.MaxValue,
            3);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.TooManyFrames);
        rejection.Value.Limit.Should().Be(3);
        rejection.Value.Actual.Should().Be(4);
    }

    [Fact]
    public void Limits_UnlimitedByDefault_AcceptEveryFrame()
    {
        ZReceiveGuard.CheckLimits(
            int.MaxValue,
            long.MaxValue,
            int.MaxValue - 1, // the int.MaxValue-th frame is still in range
            long.MaxValue,
            long.MaxValue,
            int.MaxValue).Should().BeNull();
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
        frameFirst.Should().NotBeNull();
        frameFirst.Value.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);

        // Message-total violation wins over the frame count.
        var messageFirst = ZReceiveGuard.CheckLimits(
            5,
            25,
            3,
            long.MaxValue,
            20,
            2);
        messageFirst.Should().NotBeNull();
        messageFirst.Value.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);

        // Frame-count violation wins when the earlier limits are satisfied.
        var countFirst = ZReceiveGuard.CheckLimits(
            5,
            15,
            3,
            long.MaxValue,
            20,
            2);
        countFirst.Should().NotBeNull();
        countFirst.Value.Reason.Should().Be(ZReceiveRejectionReason.TooManyFrames);
    }

    [Fact]
    public void Guard_Overflow_ReportsFailureInsteadOfThrowing()
    {
        ZReceiveGuard.TryAccumulate(long.MaxValue, 1, out _).Should().BeFalse();
        ZReceiveGuard.TryAccumulate(long.MaxValue - 1, 1, out var total).Should().BeTrue();
        total.Should().Be(long.MaxValue);
        ZReceiveGuard.TryAccumulate(41, 1, out var small).Should().BeTrue();
        small.Should().Be(42);
    }

    [Fact]
    public void QueueOptions_DefaultPolicy_IsDefaultConfiguration()
    {
        var options = new ZQueueSocketOptions();

        var policy = options.ReceivePolicy;
        policy.Should().BeOfType<ZReceiveOptions>();
        var receiveOptions = (ZReceiveOptions)policy;
        receiveOptions.Mode.Should().Be(ZReceiveMode.Pooled);
        receiveOptions.ContiguousFrameLimit.Should().Be(85_000);

        // The policy is allocation-only; limits are socket-level and default
        // to effectively unlimited.
        options.MaxFrameLength.Should().Be(long.MaxValue);
        options.MaxMessageLength.Should().Be(long.MaxValue);
        options.MaxFramesPerMessage.Should().Be(int.MaxValue);

        // The default configuration accepts a small frame pooled and contiguous.
        var allocation = policy.Decide(new ZReceiveContext { FrameLength = 100 });
        allocation.Mode.Should().Be(ZReceiveMode.Pooled);
        allocation.Segmented.Should().BeFalse();
    }
}
