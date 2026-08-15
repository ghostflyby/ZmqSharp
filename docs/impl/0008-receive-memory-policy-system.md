# 0008 - Receive Memory Policy System

Status: draft
Date: 2026-08-09

Defines the implementation plan for the receive-side memory policy system:
the allocation decision (ownership and continuity), the resource limits that
turn an allocation decision into a rejection, the rejection contract, and the
failure-class separation that keeps protocol violations from escaping as
out-of-memory or pool exceptions.

This document is a leaf plan under 0006 §3.2 ("Protocol resource limits") and
extends 0003 (receive policy) and 0005 (union-like value types). It records
the decisions already accepted in review on 2026-08-09; nothing here is
implemented yet.

## Implementation status

- Slice A is implemented: `ZReceiveRejection` /
  `ZReceiveRejectionReason` (0005 pattern), `IZReceivePolicy.Decide` returning
  a `ZReceiveAllocation` (allocation only, no rejection case), `MaxFrameLength` / `MaxMessageLength` /
  `MaxFramesPerMessage` on `ZSocketOptions` (one-time socket
  configuration, 0023) enforced by a connection-level guard in the materializer
  (fixed-order evaluation; a custom policy cannot bypass them), checked
  accumulation via `ZReceiveGuard`, terminal teardown through the existing
  failure-safe path, and a `ReceiveRejections` diagnostic counter on the
  queue surface (`ZQueueSocketBase`, 0023) (decision on open question 3:
  counter only, for now).
  Rejection is signaled by an internal `ZReceiveRejectedException`; the public
  exception hierarchy stays open per 0006 §2.3.
- Slice B is implemented: `MaxCommandSize` is an explicit option on
  `ZSocketOptions` (default 1 MiB, floor `MinMaxCommandSize` = 256 so the
  limit cannot be disabled entirely), threaded into the parser and enforced
  before the command body is read into scratch.
- Slice C (segmented materialization) is not implemented.

## 1. Context

The transport core materializes each message through a per-frame decision
hook (0003), held in its per-connection `ReceiveMaterializer`:

```text
ZSocketBase.ReceiveMaterializer.CreateAllocator
  -> policy.Decide (builds ZReceiveContext, calls IZReceivePolicy.Decide)
  -> AllocateSegments (Pool.Rent or GC.AllocateUninitializedArray)
```

The parser reads a frame header first, then invokes the allocator with the
frame length, then reads the body into the returned storage. That ordering is
the natural rejection point: a decision can reject a frame between the header
read and the body allocation, with zero body bytes read and zero storage
rented.

The current public surface:

```csharp
public enum ZReceiveMode { Pooled, Owned }

public readonly struct ZReceiveAllocation
{
    public ZReceiveMode Mode { get; init; }
    public bool Segmented { get; init; }   // v1 always false; 0003 open question
    public bool Contiguous => !Segmented;
}

public readonly struct ZReceiveContext
{
    public int FrameLength { get; init; }
    public bool HasMore { get; init; }
    public int FrameIndex { get; init; }
    public long AccumulatedLength { get; init; }
    public bool IsFirstFrame => FrameIndex == 0;
}

public interface IZReceivePolicy
{
    ZReceiveAllocation Decide(ZReceiveContext context);
}

public sealed class ZReceiveOptions : IZReceivePolicy
{
    public ZReceiveMode Mode { get; init; }
    public int ContiguousFrameLimit { get; init; } = 85_000;
}
```

`ZReceiveContext` already carries everything a limit check needs:
`FrameLength` for the single-frame limit, `AccumulatedLength` for the message
total, and `FrameIndex` for the frames-per-message limit.

## 2. Accepted Decisions

The following decisions were made during review and are binding for the
implementation.

### D1. Rejection is enforced by a connection-level guard, not the policy

The allocation decision and the resource limits are orthogonal. The numeric
limits live on `ZSocketOptions` as one-time socket configuration; each
peer's materializer checks them after the frame header is read, before any
allocation, and rejects the connection on violation.

The policy decides allocation only - it never decides whether a frame may be
received. It returns a `ZReceiveAllocation` (`Mode` + `Segmented`); there is
no Reject case and no `ZReceiveDecision` root, because the situations a
per-frame decision could reject (resource limits) are enforced by the guard,
while content-based filtering cannot be expressed at the decision point (the
frame body has not been read) and is a message-API concern (D5). Removing the
Reject case keeps a custom policy unable to bypass the limits or to reject a
frame in a way that would silently desynchronize the pipeline.

`ZReceiveRejection` is the guard's payload:

```csharp
public enum ZReceiveRejectionReason
{
    FrameTooLarge,
    MessageTooLarge,
    TooManyFrames,
}

public readonly struct ZReceiveRejection
{
    public ZReceiveRejectionReason Reason { get; init; }
    public long? Limit { get; init; }    // configured limit, when numeric
    public long? Actual { get; init; }   // observed value, when numeric
}
```

### D2. Defaults align with libzmq: unlimited unless configured

libzmq enforces no frame-size limit by default (`ZMQ_MAXMSGSIZE` defaults to
`-1`, i.e. disabled; the context-level `ZMQ_MAX_MSGSZ` defaults to
`INT_MAX`). ZMTP 3.0/3.1 defines no Max-Message-Size or Max-Command-Size
handshake metadata, so the limits are purely local policy and are never
negotiated or transmitted.

Therefore the numeric receive limits default to **effectively unlimited**
(`long.MaxValue` / `int.MaxValue`), expressed as non-null public defaults on
`ZSocketOptions` rather than nullable public fields: explicit configuration is
the user's informed opt-in. The mandatory command size limit (see D5) is the
only limit that is not opt-out.

### D3. Rejection is zero-allocation and happens at the header

- The single-frame limit is checked from `FrameLength` immediately after the
  frame header is read, before any body read.
- The message total and frames-per-message limits are checked per frame from
  the checked accumulation, still before that frame's body is read or any
  storage is rented.
- An over-limit frame must never reach `Pool.Rent`,
  `GC.AllocateUninitializedArray`, scratch growth, or segment-table creation
  (0006 §3.2 gate).
- The connection does not continue after a rejection (see D6).

### D4. The accumulated total is enforced by the pipeline, not the pool

`MemoryPool<byte>` (the injected pool, default `MemoryPool<byte>.Shared`)
has no quota or refusal contract. Its `Rent` throws
`ArgumentOutOfRangeException` only for invalid negative sizes; buffers above
its internal threshold bypass the pool and are allocated directly, and
extreme sizes surface only as `OutOfMemoryException`. The pool keeps no
cumulative accounting, knows nothing about message boundaries or connection
identity, and cannot "decide" a message total.

Consequences:

- The message total is enforced in the pipeline (the guard against checked
  `AccumulatedLength`), where the rejection can be deterministic, carry a
  reason, and terminate the peer.
- The pool remains injectable (`ZSocketOptions.Pool`), so a custom pool may
  add global accounting, but that is a separate local-resource concern and
  never the mechanism for a protocol limit.
- A protocol-limit violation must never escape as an OOM or pool exception
  (0006 §3.2 gate).

### D5. Two failure classes, one visibility rule

| Failure class | Cause | Peer-visible behavior | Local behavior |
| --- | --- | --- | --- |
| Protocol rejection | peer exceeds a configured or mandatory limit; handshake violation | handshake phase: ERROR command then close (existing path); traffic phase: close without ERROR, because ZMTP has no traffic-phase ERROR | `PeerEnded` with a rejection failure; diagnostic counter/event; never silent |
| Local allocation failure | pool exception, OOM, or custom pool refusal | plain close; the peer did nothing wrong, so no ERROR is sent | `PeerEnded` with the allocation failure; diagnostic counter; never converted into a protocol rejection |

Both classes are terminal for the connection. Neither drops frames
silently: the local side always surfaces the failure through `PeerEnded` and
a diagnostic. A consumer that wants per-message filtering must do it at the
message API, not by rejecting in the allocator.

### D6. Rejection and failure are terminal for the connection

`PeerState.FrameIndex` and `AccumulatedLength` advance before allocation
(`ZQueueSocketBase`, 0023).
Continuing a connection after a rejection would desynchronize that accounting,
so rejection always tears the connection down: the peer is removed, its
accumulated owning frames are reclaimed (0006 §3.2 gate), and the parser and
connection are disposed through the existing failure-safe `finally` path
(0006 §3.3).

## 3. Enforcement Points and Invariants

For every message frame, the parser order becomes:

```text
read frame header
  -> checked header size validation (existing, header.Size > int.MaxValue)
  -> allocator(length, more)
      -> checked accumulation; overflow reports MessageTooLarge
      -> connection guard (MaxFrameLength / MaxMessageLength /
         MaxFramesPerMessage on ZSocketOptions); violation rejects
      -> Decide: allocation only
      -> if Reject: no body read, no allocation; terminal teardown
      -> if Accept: Allocate (rent/allocate) -> read body into storage
```

Command frames keep their existing hard limit path: `MaxCommandSize`
(1 MiB) is checked before the command body is read into scratch
([ZmtpParser.cs](/Users/ghostflyby/repos/tests/ZmqSharp/ZmqSharp/Zmtp/ZmtpParser.cs:494)).

Invariants:

1. Checked accumulation: `AccumulatedLength` uses `checked` arithmetic; an
   overflow is reported as `MessageTooLarge`, never as a raw overflow
   exception.
2. No allocation precedes the guard or the decision for message frames.
3. A rejected or failed connection never remains routable and never resumes
   parsing.
4. Default behavior is unchanged: the default options mean
   Accept(Pooled, contiguous) for every frame, and the default limits are
   effectively unlimited.

## 4. Options Surface

The numeric limits are one-time socket configuration on `ZSocketOptions`
(all default to effectively unlimited, per D2), alongside the receive policy
which decides allocation only:

```csharp
public sealed class ZSocketOptions
{
    public ZQueueFactory ReceiveQueueFactory { get; init; } = new BoundedChannelOptions(16) { SingleWriter = true }; // per-peer queue (0009)
    public ZQueueFactory? SendQueueFactory { get; init; }   // optional outbound (0009)
    public IZReceivePolicy ReceivePolicy { get; init; } = new ZReceiveOptions();

    public long MaxFrameLength { get; init; }     // D2: long.MaxValue = unlimited
    public long MaxMessageLength { get; init; }   // D2: long.MaxValue = unlimited
    public int MaxFramesPerMessage { get; init; } // D2: int.MaxValue = unlimited
}
```

The guard checks the limits in a fixed order and rejects with the first
violation: `FrameTooLarge`, then `MessageTooLarge`, then `TooManyFrames`.
Numeric limit violations set both `Limit` and `Actual`.

A user-supplied `IZReceivePolicy` (delegate or custom implementation) decides
allocation only: `Decide` returns a `ZReceiveAllocation` and has no rejection
case, so the policy can neither bypass nor trigger the limits. Content-based
filtering is a message-API concern, not a policy concern (D5).

## 5. Slices

Each slice is pull-request-sized and includes focused tests plus the
documentation updates for affected statements in 0003 and this document
(0006 §8 step 1).

### Slice A - Decision result and rejection plumbing

Required work:

- Add `ZReceiveRejection`, `ZReceiveRejectionReason` as union-like value
  types (0005 pattern), produced by the connection guard.
- Change `IZReceivePolicy.Decide` to return `ZReceiveAllocation` (allocation
  only, no rejection case); update `ZReceiveOptions` and
  `ZDelegateReceivePolicy`.
- Add `MaxFrameLength`, `MaxMessageLength`, `MaxFramesPerMessage` to
  `ZSocketOptions` with the fixed-order evaluation (D1/§4) and enforce
  them with a connection-level guard in the transport core's
  `ReceiveMaterializer.CreateAllocator`.
- Add checked accumulation (`checked(AccumulatedLength + length)`) in the
  materializer's allocator; overflow maps to `MessageTooLarge`. The guard
  counters reset at each message boundary.
- Plumb rejection out of the allocator so the parser skips the body read:
  the allocator throws an internal `ZReceiveRejectedException(rejection)`,
  which propagates as the connection failure. No ERROR frame is sent for
  traffic-phase rejection.
- Teardown: rejected connections follow the existing failure-safe `finally`
  path; `PeerEnded` carries the rejection; accumulated owning frames are
  reclaimed.
- Diagnostics: add a rejection counter (or event) on `ZQueueSocketBase`; exact
  API shape stays open per 0006 §2.2.

Completion gate:

- Boundary and one-past-boundary tests for all three numeric limits.
- A probing pool whose `Rent` throws proves zero allocation on rejection
  (no `Rent` call for an over-limit frame) and normal `Rent` usage below the
  limit.
- Overflow test: two frames whose lengths overflow `long` produce
  `MessageTooLarge`, not an arithmetic exception.
- Wire-level test: traffic-phase rejection closes without an ERROR frame;
  handshake-phase violations still send ERROR (existing behavior).
- After rejection: `PeerEnded` raised with the rejection failure, the peer is
  not routable, no further frames are delivered, and owning storage is
  returned to the pool (observable via the probing pool).
- Default unlimited: without limit configuration, existing behavior is
  unchanged and all tests pass.

### Slice B - Configurable command size (implemented)

Move `MaxCommandSize` from a parser constant into an explicit option with a
mandatory default of 1 MiB (0006 §3.2 lists "maximum command size" as an
explicit option). Enforcement stays in the parser and remains non-opt-out
beyond the configured value.

Implemented: `ZSocketOptions.MaxCommandSize` (default 1 MiB) with the
construction-time floor `ZSocketOptions.MinMaxCommandSize` (256); the parser
takes the limit via the transport core and rejects an oversized command
before reading its body into scratch. Tests cover an oversized command frame
rejecting the peer and the below-floor validation throwing.

Completion gate:

- Boundary and one-past-boundary tests for the configured command size.
- A configured floor prevents disabling the limit entirely (validation at
  option construction).

### Slice C - Segmented as a Decide-autonomous decision (deferred, 0003)

`Segmented` remains a field of `ZReceiveAllocation` that `Decide` chooses per
frame. Segment-size configuration, segment-table growth checks, and the
contiguous threshold semantics stay with the 0003 design track. Slice A does
not depend on it: rejection checks run before any segmentation decision.

## 6. Non-Goals

- Send-side memory policy.
- Borrowed-tier policy: the low-level callback keeps receiving contiguous
  borrowed frames.
- Per-peer policy contexts: `Decide` carries message information only;
  connection identity is a later slice (0003 non-goal, ROUTER).
- Pool-side quotas: the BCL pool contract has no refusal convention (D4);
  global memory accounting by a custom pool is out of scope for this plan.
- Automatic segmentation implementation (Slice C).

## 7. Open Questions for Review

1. Limit types: non-null `long.MaxValue` / `int.MaxValue` defaults on
   `ZSocketOptions` (this plan) versus a libzmq-style `-1` sentinel or
   nullable fields. The plan uses non-null defaults so the options are
   declarative and require no null handling.
2. Whether `MaxFramesPerMessage` belongs with the other limits or in a
   message-layer policy. The plan keeps it with the limits because
   `ZReceiveContext.FrameIndex` already provides the count at the guard point.
3. Diagnostic API shape: counter, event, or both; and whether the rejection
   reason should be a dedicated exception type or a
   `ZeroMqProtocolException` subclass with the reason attached. Slice A landed
   a `ReceiveRejections` counter plus the internal
   `ZReceiveRejectedException`; revisiting this is open.
4. Whether the fixed evaluation order (frame, message total, frame count) is
   the desired precedence when multiple limits are exceeded.
