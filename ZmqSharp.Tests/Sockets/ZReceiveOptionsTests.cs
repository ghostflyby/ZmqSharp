using FluentAssertions;
using Xunit;
using ZmqSharp.Sockets;

namespace ZmqSharp.Tests;

/// <summary>
/// Unit tests for the receive decision root and the numeric limits of
/// <see cref="ZReceiveOptions"/> (0008 D1/D2/D4), plus the checked
/// accumulation guard (0008 D3/D6).
/// </summary>
public sealed class ZReceiveOptionsTests
{
    [Fact]
    public void Decision_AcceptCase_ExposesOnlyAllocation()
    {
        var decision = new ZReceiveDecision(new ZReceiveAllocation { Mode = ZReceiveMode.Owned });

        decision.TryGetValue(out ZReceiveAllocation allocation).Should().BeTrue();
        allocation.Mode.Should().Be(ZReceiveMode.Owned);
        decision.TryGetValue(out ZReceiveRejection _).Should().BeFalse();
    }

    [Fact]
    public void Decision_RejectCase_ExposesOnlyRejection()
    {
        var decision = new ZReceiveDecision(new ZReceiveRejection { Reason = ZReceiveRejectionReason.Policy });

        decision.TryGetValue(out ZReceiveRejection rejection).Should().BeTrue();
        rejection.Reason.Should().Be(ZReceiveRejectionReason.Policy);
        decision.TryGetValue(out ZReceiveAllocation _).Should().BeFalse();
    }

    [Fact]
    public void FrameLimit_AtLimitAccepts_OnePastRejects()
    {
        // Limits are enforced by the connection-level guard, not the policy.
        ZReceiveGuard.CheckLimits(
            frameLength: 100,
            accumulatedLength: 100,
            frameIndex: 0,
            maxFrameLength: 100,
            maxMessageLength: long.MaxValue,
            maxFramesPerMessage: int.MaxValue).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            frameLength: 101,
            accumulatedLength: 101,
            frameIndex: 0,
            maxFrameLength: 100,
            maxMessageLength: long.MaxValue,
            maxFramesPerMessage: int.MaxValue);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        rejection.Value.Limit.Should().Be(100);
        rejection.Value.Actual.Should().Be(101);
    }

    [Fact]
    public void MessageLimit_AtLimitAccepts_OnePastRejects()
    {
        ZReceiveGuard.CheckLimits(
            frameLength: 1,
            accumulatedLength: 100,
            frameIndex: 0,
            maxFrameLength: long.MaxValue,
            maxMessageLength: 100,
            maxFramesPerMessage: int.MaxValue).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            frameLength: 1,
            accumulatedLength: 101,
            frameIndex: 0,
            maxFrameLength: long.MaxValue,
            maxMessageLength: 100,
            maxFramesPerMessage: int.MaxValue);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);
        rejection.Value.Limit.Should().Be(100);
        rejection.Value.Actual.Should().Be(101);
    }

    [Fact]
    public void FramesPerMessage_AtLimitAccepts_OnePastRejects()
    {
        ZReceiveGuard.CheckLimits(
            frameLength: 1,
            accumulatedLength: 3,
            frameIndex: 2,
            maxFrameLength: long.MaxValue,
            maxMessageLength: long.MaxValue,
            maxFramesPerMessage: 3).Should().BeNull();

        var rejection = ZReceiveGuard.CheckLimits(
            frameLength: 1,
            accumulatedLength: 4,
            frameIndex: 3,
            maxFrameLength: long.MaxValue,
            maxMessageLength: long.MaxValue,
            maxFramesPerMessage: 3);
        rejection.Should().NotBeNull();
        rejection.Value.Reason.Should().Be(ZReceiveRejectionReason.TooManyFrames);
        rejection.Value.Limit.Should().Be(3);
        rejection.Value.Actual.Should().Be(4);
    }

    [Fact]
    public void Limits_UnlimitedByDefault_AcceptEveryFrame()
    {
        ZReceiveGuard.CheckLimits(
            frameLength: int.MaxValue,
            accumulatedLength: long.MaxValue,
            frameIndex: int.MaxValue - 1, // the int.MaxValue-th frame is still in range
            maxFrameLength: long.MaxValue,
            maxMessageLength: long.MaxValue,
            maxFramesPerMessage: int.MaxValue).Should().BeNull();
    }

    [Fact]
    public void Limits_EvaluateInFixedOrder_FirstViolationWins()
    {
        // Frame violation wins over the message total.
        var frameFirst = ZReceiveGuard.CheckLimits(
            frameLength: 11,
            accumulatedLength: 11,
            frameIndex: 0,
            maxFrameLength: 10,
            maxMessageLength: 20,
            maxFramesPerMessage: 2);
        frameFirst.Should().NotBeNull();
        frameFirst.Value.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);

        // Message-total violation wins over the frame count.
        var messageFirst = ZReceiveGuard.CheckLimits(
            frameLength: 5,
            accumulatedLength: 25,
            frameIndex: 3,
            maxFrameLength: long.MaxValue,
            maxMessageLength: 20,
            maxFramesPerMessage: 2);
        messageFirst.Should().NotBeNull();
        messageFirst.Value.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);

        // Frame-count violation wins when the earlier limits are satisfied.
        var countFirst = ZReceiveGuard.CheckLimits(
            frameLength: 5,
            accumulatedLength: 15,
            frameIndex: 3,
            maxFrameLength: long.MaxValue,
            maxMessageLength: 20,
            maxFramesPerMessage: 2);
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
        policy.Decide(new ZReceiveContext { FrameLength = 100 })
            .TryGetValue(out ZReceiveAllocation allocation).Should().BeTrue();
        allocation.Mode.Should().Be(ZReceiveMode.Pooled);
        allocation.Segmented.Should().BeFalse();
    }
}
