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

## 1. Context

The queue tier materializes each message through a per-frame decision hook
(0003):

```text
ZQueueSocket.CreateAllocator
  -> DecideAllocation (builds ZReceiveContext, calls IZReceivePolicy.Decide)
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

### D1. Rejection is a third allocation-decision result

`Decide` returns a decision root, not a bare allocation. Following the 0005
union-like pattern, the root carries exactly one case:

```csharp
public readonly struct ZReceiveDecision
{
    public bool TryGetValue(out ZReceiveAllocation allocation); // Accept case
    public bool TryGetValue(out ZReceiveRejection rejection);   // Reject case
}
```

`ZReceiveAllocation` remains the Accept payload (`Mode` + `Segmented`).
`ZReceiveRejection` is the Reject payload:

```csharp
public enum ZReceiveRejectionReason
{
    FrameTooLarge,
    MessageTooLarge,
    TooManyFrames,
    Policy, // arbitrary policy decision
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

Therefore the numeric receive limits default to **unlimited** (`null`):
explicit configuration is the user's informed opt-in. The mandatory command
size limit (see D5) is the only limit that is not opt-out.

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

- The message total is enforced in the pipeline (`Decide` against checked
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
([ZQueueSocket.cs](/Users/ghostflyby/repos/tests/ZmqSharp/ZmqSharp/Sockets/ZQueueSocket.cs:250)).
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
  -> allocator(length, more)      // calls Decide; may reject
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
2. No allocation precedes the decision for message frames.
3. A rejected or failed connection never remains routable and never resumes
   parsing.
4. Default behavior is unchanged: no policy and no limits configured means
   Accept(Pooled, contiguous) for every frame.

## 4. Proposed Options Surface

`ZReceiveOptions` gains numeric limits (all default `null` = unlimited, per
D2):

```csharp
public sealed class ZReceiveOptions : IZReceivePolicy
{
    public ZReceiveMode Mode { get; init; }
    public int ContiguousFrameLimit { get; init; } = 85_000;

    public long? MaxFrameLength { get; init; }     // D2: null = unlimited
    public long? MaxMessageLength { get; init; }   // D2: null = unlimited
    public int? MaxFramesPerMessage { get; init; } // D2: null = unlimited
}
```

`Decide` evaluates the limits in a fixed order and returns the first
rejection: `FrameTooLarge`, then `MessageTooLarge`, then `TooManyFrames`,
then the user's own policy decision. Numeric limit violations set both
`Limit` and `Actual`.

Where the policy is a user-supplied `IZReceivePolicy` (delegate or custom
implementation), it is responsible for its own limit decisions; the numeric
configuration in `ZReceiveOptions` is just the built-in policy implementation.

## 5. Slices

Each slice is pull-request-sized and includes focused tests plus the
documentation updates for affected statements in 0003 and this document
(0006 §8 step 1).

### Slice A - Decision result and rejection plumbing

Required work:

- Add `ZReceiveDecision`, `ZReceiveRejection`,
  `ZReceiveRejectionReason` as union-like value types (0005 pattern).
- Change `IZReceivePolicy.Decide` to return `ZReceiveDecision`; update
  `ZReceiveOptions` and `ZDelegateReceivePolicy`.
- Add `MaxFrameLength`, `MaxMessageLength`, `MaxFramesPerMessage` to
  `ZReceiveOptions` with the fixed-order evaluation in D4/§4.
- Add checked accumulation (`checked(AccumulatedLength + length)`) in
  `CreateAllocator`; overflow maps to `MessageTooLarge`.
- Plumb rejection out of the allocator so the parser skips the body read:
  the allocator throws an internal `ZReceiveRejectedException(rejection)`,
  which propagates as the connection failure. No ERROR frame is sent for
  traffic-phase rejection.
- Teardown: rejected connections follow the existing failure-safe `finally`
  path; `PeerEnded` carries the rejection; accumulated owning frames are
  reclaimed.
- Diagnostics: add a rejection counter (or event) on `ZQueueSocket`; exact
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

### Slice B - Configurable command size (optional, small)

Move `MaxCommandSize` from a parser constant into an explicit option with a
mandatory default of 1 MiB (0006 §3.2 lists "maximum command size" as an
explicit option). Enforcement stays in the parser and remains non-opt-out
beyond the configured value.

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

1. Limit types: `long?` with `null` = unlimited (this plan) versus a
   libzmq-style `-1` sentinel. The plan prefers nullable types for
   self-documentation.
2. Whether `MaxFramesPerMessage` belongs here or in a message-layer policy;
   the plan places it here because `ZReceiveContext.FrameIndex` already
   provides the count at the decision point.
3. Diagnostic API shape: counter, event, or both; and whether the rejection
   reason should be a dedicated exception type or a
   `ZeroMqProtocolException` subclass with the reason attached.
4. Whether the fixed evaluation order (frame, message total, frame count,
   policy) is the desired precedence when multiple limits are exceeded.
