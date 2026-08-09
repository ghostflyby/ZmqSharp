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
        var policy = new ZReceiveOptions { MaxFrameLength = 100 };

        policy.Decide(new ZReceiveContext { FrameLength = 100 })
            .TryGetValue(out ZReceiveAllocation _).Should().BeTrue();

        var decision = policy.Decide(new ZReceiveContext { FrameLength = 101 });
        decision.TryGetValue(out ZReceiveRejection rejection).Should().BeTrue();
        rejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);
        rejection.Limit.Should().Be(100);
        rejection.Actual.Should().Be(101);
    }

    [Fact]
    public void MessageLimit_AtLimitAccepts_OnePastRejects()
    {
        var policy = new ZReceiveOptions { MaxMessageLength = 100 };

        policy.Decide(new ZReceiveContext { FrameLength = 100, AccumulatedLength = 100 })
            .TryGetValue(out ZReceiveAllocation _).Should().BeTrue();

        var decision = policy.Decide(new ZReceiveContext { FrameLength = 1, AccumulatedLength = 101 });
        decision.TryGetValue(out ZReceiveRejection rejection).Should().BeTrue();
        rejection.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);
        rejection.Limit.Should().Be(100);
        rejection.Actual.Should().Be(101);
    }

    [Fact]
    public void FramesPerMessage_AtLimitAccepts_OnePastRejects()
    {
        var policy = new ZReceiveOptions { MaxFramesPerMessage = 3 };

        policy.Decide(new ZReceiveContext { FrameIndex = 2 })
            .TryGetValue(out ZReceiveAllocation _).Should().BeTrue();

        var decision = policy.Decide(new ZReceiveContext { FrameIndex = 3 });
        decision.TryGetValue(out ZReceiveRejection rejection).Should().BeTrue();
        rejection.Reason.Should().Be(ZReceiveRejectionReason.TooManyFrames);
        rejection.Limit.Should().Be(3);
        rejection.Actual.Should().Be(4);
    }

    [Fact]
    public void Limits_UnlimitedByDefault_AcceptEveryFrame()
    {
        var policy = new ZReceiveOptions();

        policy.Decide(new ZReceiveContext { FrameLength = int.MaxValue, AccumulatedLength = long.MaxValue })
            .TryGetValue(out ZReceiveAllocation allocation).Should().BeTrue();
        allocation.Mode.Should().Be(ZReceiveMode.Pooled);
        allocation.Segmented.Should().BeTrue(); // above the contiguous threshold
    }

    [Fact]
    public void Limits_EvaluateInFixedOrder_FirstViolationWins()
    {
        var policy = new ZReceiveOptions
        {
            MaxFrameLength = 10,
            MaxMessageLength = 20,
            MaxFramesPerMessage = 2,
        };

        // Frame violation wins over the message total.
        var frameFirst = policy.Decide(new ZReceiveContext { FrameLength = 11, AccumulatedLength = 11 });
        frameFirst.TryGetValue(out ZReceiveRejection frameRejection).Should().BeTrue();
        frameRejection.Reason.Should().Be(ZReceiveRejectionReason.FrameTooLarge);

        // Message-total violation wins over the frame count.
        var messageFirst = policy.Decide(new ZReceiveContext
        {
            FrameLength = 5,
            AccumulatedLength = 25,
            FrameIndex = 3,
        });
        messageFirst.TryGetValue(out ZReceiveRejection messageRejection).Should().BeTrue();
        messageRejection.Reason.Should().Be(ZReceiveRejectionReason.MessageTooLarge);

        // Frame-count violation wins when the earlier limits are satisfied.
        var countFirst = policy.Decide(new ZReceiveContext { FrameLength = 5, AccumulatedLength = 15, FrameIndex = 3 });
        countFirst.TryGetValue(out ZReceiveRejection countRejection).Should().BeTrue();
        countRejection.Reason.Should().Be(ZReceiveRejectionReason.TooManyFrames);
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
        var policy = new ZQueueSocketOptions().ReceivePolicy;

        policy.Should().BeOfType<ZReceiveOptions>();
        var options = (ZReceiveOptions)policy;
        options.Mode.Should().Be(ZReceiveMode.Pooled);
        options.ContiguousFrameLimit.Should().Be(85_000);
        options.MaxFrameLength.Should().BeNull();
        options.MaxMessageLength.Should().BeNull();
        options.MaxFramesPerMessage.Should().BeNull();

        // The default configuration accepts a small frame pooled and contiguous.
        policy.Decide(new ZReceiveContext { FrameLength = 100 })
            .TryGetValue(out ZReceiveAllocation allocation).Should().BeTrue();
        allocation.Mode.Should().Be(ZReceiveMode.Pooled);
        allocation.Segmented.Should().BeFalse();
    }
}
