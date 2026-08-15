# 0019 - Inbound Decision and Socket Composition Surface

Status: accepted
Date: 2026-08-13
Revision: 2

Completes the pattern-core split begun by 0015 section 2. 0015 split
`IPatternCore` into an outbound seam (`IZDispatchPolicy`) and an identity
seam (`ZSocketType`); this document adds the missing inbound seam and turns
the socket's composition face into a usable third-party surface.

- **Inbound behavior has no seam.** Filtering, framing, forwarding, and
  protocol consumption are still scattered across `ZSocketBase` virtuals
  (`PrepareInboundForSink`), the `IPatternSink` interception of REQ/REP, and
  socket overrides (SUB filter, ROUTER identity prefix, XPUB subscription
  forwarding, XSUB passthrough). Two different mechanisms express the same
  decision: `ZMessage?` for deliver-or-drop, `IPatternSink` for consume.
- **The composition face is internal.** `ZSocketBase`'s constructor takes
  the seams but is `internal`, so only the library (and the test project via
  `InternalsVisibleTo`) can build a socket type. A third-party custom type
  (0015 section 2.3) cannot subclass it.
- **Extraction must serve the user's combination face, not the library's
  extraction face.** Shared line-format helpers and lifecycle plumbing that
  no new socket type needs to combine stay internal; only what a developer
  assembling a new socket type must decide is public.

## 1. Problem

The outbound seam proved the shape: a neutral decision interface the base
executes. The inbound half never got one, so pattern behavior still lives in
three places at once:

- `ZSocketBase.PrepareInboundForSink` returns `ZMessage?` - deliver-or-drop,
  used by ROUTER (identity prefix), SUB (topic filter), XSUB (passthrough),
  XPUB (subscription forwarding).
- REQ and REP implement `IPatternSink` and bind themselves as the socket's
  sink, intercepting inbound messages to consume them (delimiter stripping,
  current-peer gate, request slot). This hijacks the public delivery surface:
  a `ZSocketOptions.MessageSink` consumer can never attach to a REQ.
- `ZSocketBase.OnPatternPeerEnded` (whose name still says "Pattern") is the
  teardown hook for per-pattern state such as ROUTER's identity map.

The result is that "how a socket responds" is not assembled anywhere - it is
the sum of a virtual, a sink interception, and an override. The three-state
nature of the decision (deliver / drop / consume) is implicit.

A second, independent observation from the same review: the compatibility
matrix (0015 section 2.2) is a many-to-many asymmetric graph, not a set of
one-to-one pairs (REQ accepts only REP, but REP also accepts DEALER; ROUTER
accepts DEALER/REQ/ROUTER). Shared behavior therefore belongs to *families*
of mutually compatible types, not to pairs - and the shared fragments are
small (delimiter framing across REQ/REP, subscription frames across SUB/XSUB,
the subscription line protocol across PUB/SUB/XPUB/XSUB).

## 2. Composition face

A new socket type is a subclass of `ZSocketBase` plus one constructor. The
protected constructor is therefore the whole composition surface, and it must
exactly contain what a developer decides:

```csharp
public abstract class ZSocketBase : ZAsyncState, IZCallbackSocket
{
    protected ZSocketBase(
        ZSocketOptions options,
        IZDispatchPolicy dispatch,          // outbound: who receives a sent message
        ZSocketType type,                   // identity: advertised name + who may connect
        IZInboundPolicy? inbound = null);   // inbound: what happens to a received message
}
```

Three decisions, matching three questions a developer answers to define a
socket type: **what do I send to (dispatch), what do I do with what arrives
(inbound), and what do I call myself / who do I accept (type)**. `inbound`
defaults to pass-through delivery, so sockets that only need outbound routing
(single-peer, round-robin, broadcast) declare nothing inbound. The base
executes the seams; the socket type only composes them.

Caller-addressed sends are deliberately outside the seams: REP replies route
back to the originating peer and ROUTER sends address a peer by identity.
Both are directed sends (`SendToAsync(peer, message)`) where the caller has
already specified the target - they are not policy decisions and stay base
primitives.

## 3. Inbound decision: three states

Inbound processing ends in exactly one of three states, which is the single
exit point the base switches on:

```csharp
public enum ZInboundAction
{
    Deliver,   // hand the message to the bound sink; may carry a replacement
    Drop,      // the policy disposed the message; nothing is delivered
    Consumed   // the policy took the message (protocol consumption); nothing is delivered
}

public readonly struct ZInboundDecision
{
    public ZInboundAction Action { get; init; }

    /// <summary>Deliver only: the replacement message (frames moved, 0007 M3);
    /// null delivers the original message untouched.</summary>
    public ZMessage? Message { get; init; }
}

public interface IZInboundPolicy
{
    ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token);
}
```

Async because consumption is async: REP's request slot wait and handler call,
and REQ's reply completion, both live inside `DecideAsync`. Sync filters
return `ValueTask.FromResult`.

Ownership contract (the library's move rules, 0007 M3):

| State | Original message | Result |
|---|---|---|
| `Deliver`, `Message == null` | passes through, base does not dispose | sink receives the original and disposes it |
| `Deliver`, `Message != null` | frames moved into the replacement; the policy must not dispose it | sink receives the replacement and disposes it |
| `Drop` | the policy disposed it (or its frames) | nothing |
| `Consumed` | the policy owns it completely | nothing |

The delegate wrapper mirrors `ZDecide` / `ZDelegateReceivePolicy` so simple
filters need no class:

```csharp
public delegate ValueTask<ZInboundDecision> ZInboundDecide(
    IZConnection peer, ZMessage message, CancellationToken token);

public sealed class ZDelegateInboundPolicy(ZInboundDecide decide) : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
        => decide(peer, message, token);
}
```

## 4. Base integration

- The receive pipeline has two tiers, unchanged in spirit. The **borrowed
  tier** (`OnFrame`) delivers raw frames and runs no inbound policy. The
  **aggregated tier** accumulates frames into a message and runs the policy;
  it activates when a message sink is bound **or** the socket composes a
  non-default inbound policy (protocol sockets such as REQ/REP need the
  aggregation but no public sink). A `Deliver` decision on the aggregated
  tier with no bound sink drops the message.
- A custom sink is configured at construction through
  `ZSocketOptions.MessageSink` (0023): `BindMessageSink` is now an internal
  seam used only by `ZQueueSocketBase` to bind its own `QueueSurface`; user
  sinks are constructor configuration, so set-once is enforced by the
  language. Protocol sockets (REQ/REP) stop implementing `IPatternSink` and
  stop binding themselves, so the seam is no longer hijacked.
- The aggregated tier tail becomes:

  ```csharp
  var decision = await inbound.DecideAsync(connection, message, token);
  if (decision.Action == ZInboundAction.Deliver)
      await messageSink.OnMessageAsync(connection, decision.Message ?? message, token);
  ```

- `PrepareInboundForSink` and `OnPatternPeerEnded` are removed (superseded by
  the inbound seam and the internal lifecycle capability, section 6).

## 5. Per-type assembly

The eleven built-in types are each one row: outbound dispatch, inbound
behavior, private state. Built-ins that do not send (PULL, SUB, XSUB) or
whose sends are directed (ROUTER, REQ, REP) deny the generic send path with a
per-type reason.

| Type | Outbound | Inbound | Private state |
|---|---|---|---|
| PAIR | `ZSinglePeerDispatch` | deliver passthrough | - |
| DEALER | `ZRoundRobinDispatch` | deliver passthrough | RR cursor |
| PUSH | `ZRoundRobinDispatch` | deliver passthrough | RR cursor |
| PULL | deny ("receive-only") | deliver passthrough | - |
| REQ | `ZCurrentPeerDispatch` (routes current) over `ZRoundRobinDispatch` (selects next) | consume (current-peer check, delimiter strip, reply completion) | current, pending gate |
| REP | deny ("replies through SendReplyAsync") | consume (slot, delimiter strip, request handler) | request slot |
| ROUTER | `ZIdentityDispatch` (identity table) | deliver with identity prefix | identity table |
| PUB | `ZBroadcastDispatch` | deliver passthrough | - |
| SUB | deny ("receive-only") | deliver/drop (topic filter) | subscriptions |
| XPUB | `ZBroadcastDispatch` | deliver + forward subscription frames | - |
| XSUB | deny ("receive-only") | deliver passthrough | - |

ROUTER and REQ demonstrate the shape fully: ROUTER's identity routing
*and* its inbound identity prefix both live in `ZIdentityDispatch` (the
prefix is assigned by the policy the socket delegates to); REQ's inbound
consume and outbound current-routing share the same in-flight gate through
`ZCurrentPeerDispatch`.

## 6. Building blocks

Public (combinable by a new socket type):

- `IZDispatchPolicy` and `ZRoundRobinDispatch`, `ZSinglePeerDispatch`,
  `ZBroadcastDispatch`, `ZIdentityDispatch`, `ZCurrentPeerDispatch`.
- `IZInboundPolicy`, `ZInboundAction`, `ZInboundDecision` (with its
  `Deliver`/`Drop`/`Consumed` factories), `ZInboundDecide`,
  `ZDelegateInboundPolicy`, and `ZInboundPolicy.PassThrough` (the default
  inbound when a socket composes none).
- `ZSocketType`, `ZSocketTypes`, plus a new convenience constructor:

  ```csharp
  // Custom types interop only between ZmqSharp endpoints advertising the
  // same name (0015 section 2.3); this is that exact predicate, without the
  // boilerplate.
  public static ZSocketType ForCustom(string name);   // AcceptsPeer: t => t == name
  ```

Internal (built-in assembly, not API):

- `ZDelimiterFraming` - the empty-delimiter wire format shared by REQ and REP
  (encode / decode; the current `ZReqCore`/`ZRepCore` bodies are verbatim
  duplicates, the same shape of duplication 0015 section 1 called out for
  round-robin). The library's `ZRepCore` currently references
  `ZReqCore.EmptyFrame`; the shared type removes that coupling.
- `ZSubscriptionFrames` - the libzmq subscription wire convention
  (`0x01` + topic subscribe, `0x00` + topic unsubscribe) shared by SUB,
  XSUB, and XPUB's forwarding.
- `ZTopicFilter` - SUB's subscription set and prefix match.
- `IZPeerLifecycle` - peer add/remove notifications for stateful policies
  (`ZIdentityDispatch` identity release, `ZCurrentPeerDispatch` current
  clearing); the base detects it on the composed dispatch. Internal because
  only built-in policies need it today; promoted if a custom dispatch policy
  surfaces the need.
- `ZNoDispatch` and the REQ/REP gate coordinators (`ZReqCore`, `ZRepCore`),
  which implement the consume side of the inbound seam.

## 7. Minimal combinations

The surface is minimal when the developer does nothing unnecessary: no
inbound (default passthrough), no lifecycle, no line-format knowledge. A
custom socket author imports `ZmqSharp` (base) and `ZmqSharp.Patterns`
(seam arguments), exactly as a mechanism author imports `ZmqSharp.Security`.

**Echo endpoint (outbound single-target, inbound passthrough) - two lines:**

```csharp
sealed class EchoSocket(ZSocketOptions options)
    : ZSocketBase(options, new ZSinglePeerDispatch(), ZSocketType.ForCustom("ECHO"));
```

**Filtered broadcast (PUB variant, one filter lambda):**

```csharp
sealed class FilterPubSocket(ZSocketOptions options)
    : ZSocketBase(options, new ZBroadcastDispatch(), ZSocketTypes.Pub,
        new ZDelegateInboundPolicy(FilterByTopic));

static ValueTask<ZInboundDecision> FilterByTopic(IZConnection peer, ZMessage message, CancellationToken token)
{
    if (MatchTopic(message)) return ValueTask.FromResult(ZInboundDecision.Deliver());

    // The policy owns the message on Drop: dispose it before returning.
    message.Dispose();
    return ValueTask.FromResult(ZInboundDecision.Drop());
}
```

**Custom request-reply (outbound fair-queue, inbound consume) - one stateful
inbound policy:**

```csharp
sealed class MyReqSocket(ZSocketOptions options)
    : ZSocketBase(options, new ZRoundRobinDispatch(), ZSocketType.ForCustom("MYREQ"),
        new MyReqInbound());

sealed class MyReqInbound : IZInboundPolicy
{
    public ValueTask<ZInboundDecision> DecideAsync(IZConnection peer, ZMessage message, CancellationToken token)
    {
        // Consume: complete this socket's pending request, or drop a spurious
        // message (ZInboundAction.Consumed owns the message).
    }
}
```

## 8. Compatibility and behavior preservation

- The built-in eleven keep identical observable behavior for the sink and
  interop paths: the same outbound routes, the same accept matrix (locked by
  the NetMQ interop suite, 0015 section 2.4), the same inbound filtering and
  framing.
- One surface is superseded for protocol sockets (ROUTER, SUB, XPUB, and the
  consume-only REQ/REP): their `OnFrame` raw-frame surface no longer fires,
  because a non-default inbound policy consumes the aggregated delivery
  stream. Subscribing to `OnFrame` on such a socket now throws instead of
  silently delivering nothing (section 11).
- The change is structural: the decision logic moves into the inbound seam,
  the delivery mechanics stay in the base. No wire bytes change.
- Custom socket types interoperate only between ZmqSharp endpoints
  advertising the same name (0015 section 2.3); `ForCustom` is the
  constructor for exactly that contract.

## 9. Rejected alternative: chain of responsibility

Evaluated and rejected. CoR is "candidate handlers, the first that can handle
it wins" (UI bubbling, middleware). Pattern inbound is not that: each type
has at most one inbound fragment and the decision is a deterministic
three-state, never a search for a handler. CoR handlers are also decoupled
through the request object, but pattern decisions need type-specific state
(SUB's subscriptions, REQ's pending gate), so a chain would degrade into
`is`-casting per handler. What resembles a chain is a short-circuit pipeline
(fixed-order transforms, any may terminate) - and even that is overbuilt for
per-type single-fragment decisions. Combination here is compositional
(assemble fragments per type), not sequential (walk a chain).

## 10. Work items

| # | Item | Size | Notes |
|---|------|------|-------|
| 1 | Inbound seam: `IZInboundPolicy`/`ZInboundAction`/`ZInboundDecision`/`ZInboundDecide`/`ZDelegateInboundPolicy`, base ctor becomes `protected` with default inbound, remove `PrepareInboundForSink`/`OnPatternPeerEnded` and the REQ/REP `IPatternSink` interception | Medium | The core deliverable |
| 2 | Internal assembly: `ZDelimiterFraming` (dedupe REQ/REP), `ZSubscriptionFrames`, `ZTopicFilter`, `IZPeerLifecycle` | Medium | Pure extraction; behavior identical |
| 3 | `ZSocketType.ForCustom` and policy/inbound seam tests | Small | Custom-type scenario tests (0015 section 2.3) |

Ordering: 1 first - it is the actual third-party composition surface; 2 is
safe extraction that removes the second verbatim duplication; 3 closes the
custom-type story. The seams land in the `ZmqSharp.Patterns` sub-namespace
(0018 section 5, resolved): dispatch, inbound, and type are the extension
face for custom socket types, imported like `ZmqSharp.Security` by the
extension author; factory users never write these types and do not pay the
import.

## 11. Implemented (revision 2)

All three work items landed. Deviations from the draft, recorded honestly:

- **`ZSubscriptionFrames` was not extracted.** SUB/XSUB/XPUB's subscription
  frame construction stayed as small local helpers; it is not a public seam
  and extracting it added no behavior. The subscription set and filter were
  extracted as `ZTopicFilter` + `ZTopicFilterPolicy` (internal).
- **`IZPeerLifecycle` was deferred.** Peer teardown notifications already
  exist as the public `PeerEnded` event; ROUTER and REQ subscribe to it.
  Promoted only if a custom dispatch policy surfaces the need.
- **XPUB's forwarding policy holds a back-reference.** The `XPubInbound`
  policy is attached after the base constructor (before any connection); it
  is an internal implementation detail of the built-in socket.
- **Deliver without a bound sink disposes the message** (documented in the
  aggregated-tier path); the pass-through tier over `OnFrame` is unaffected.
- **`OnFrame` on protocol sockets now throws** (revision 2, subagent review
  finding): a non-default inbound policy (ROUTER, SUB, XPUB, REQ, REP) always
  aggregates, so subscribing to the raw frame surface on those sockets would
  silently deliver nothing; the `OnFrame` add path throws instead. This
  replaces the pre-refactor REQ/REP behavior (their self-binding)
  interception made `OnFrame` throw) and the pre-refactor ROUTER/SUB/XPUB
  behavior (raw frames without filtering/prefixing/forwarding).
- The base constructor is `protected` and is the third-party composition
  face; custom types validate with in-library pair tests only (0015
  section 2.3), since libzmq/NetMQ hard-code their socket types.
