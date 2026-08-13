using ZmqSharp.Transports;

namespace ZmqSharp.Patterns;

/// <summary>
/// The terminal state of inbound processing (0019 section 3): a received
/// message is either delivered to the bound sink, dropped by the policy, or
/// consumed by a protocol (REQ completing its pending request, REP entering
/// its request slot). This is the single exit point the base switches on.
/// </summary>
public enum ZInboundAction
{
    /// <summary>
    /// Hand the message to the bound sink; may carry a replacement message
    /// (frames moved, 0007 M3).
    /// </summary>
    Deliver,

    /// <summary>The policy disposed the message; nothing is delivered.</summary>
    Drop,

    /// <summary>The policy took the message (protocol consumption); nothing is delivered.</summary>
    Consumed
}

/// <summary>An inbound decision: the action plus, for Deliver, an optional replacement message.</summary>
public readonly struct ZInboundDecision
{
    public ZInboundAction Action { get; init; }

    /// <summary>
    /// Deliver only: the replacement message, with the original message's
    /// frames moved (0007 M3) - null delivers the original message untouched.
    /// </summary>
    public ZMessage? Message { get; init; }

    /// <summary>Delivers the message unchanged to the bound sink.</summary>
    public static ZInboundDecision Deliver() => new() { Action = ZInboundAction.Deliver };

    /// <summary>Delivers the replacement message (the original message's frames moved, 0007 M3).</summary>
    public static ZInboundDecision Deliver(ZMessage message) => new() { Action = ZInboundAction.Deliver, Message = message };

    /// <summary>Drops the message; the policy must dispose it before returning.</summary>
    public static ZInboundDecision Drop() => new() { Action = ZInboundAction.Drop };

    /// <summary>Consumes the message; the policy owns it completely.</summary>
    public static ZInboundDecision Consumed() => new() { Action = ZInboundAction.Consumed };
}

/// <summary>
/// Inbound selection only (0019 section 3): decides what happens to a
/// received message - deliver (optionally transformed), drop, or consume.
/// The base runs the policy on every aggregated message; protocol sockets
/// (REQ, REP) implement the consume side, content sockets (SUB, ROUTER,
/// XPUB) the deliver/drop side, and pass-through sockets compose
/// <see cref="ZInboundPolicy.PassThrough"/>.
/// </summary>
public interface IZInboundPolicy
{
    /// <summary>Decides the fate of <paramref name="message"/>; ownership follows the action
    /// (see <see cref="ZInboundDecision"/>).</summary>
    ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token);
}

/// <summary>Wraps a decide delegate as an inbound policy.</summary>
public sealed class ZDelegateInboundPolicy(ZInboundDecide decide) : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        return decide(peer, message, token);
    }
}

/// <summary>Decides the fate of a received message, with the originating peer.</summary>
public delegate ValueTask<ZInboundDecision> ZInboundDecide(
    IZConnection peer, ZMessage message, CancellationToken token);

/// <summary>Ready-made inbound policies.</summary>
public static class ZInboundPolicy
{
    /// <summary>
    /// Pass-through delivery: every message reaches the bound sink untouched.
    /// The default when a socket composes no inbound policy.
    /// </summary>
    public static IZInboundPolicy PassThrough { get; } = new ZDelegateInboundPolicy(
        (_, _, _) => ValueTask.FromResult(ZInboundDecision.Deliver()));
}
