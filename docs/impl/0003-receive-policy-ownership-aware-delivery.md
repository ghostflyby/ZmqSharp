# 0003 - Receive Policy: Ownership- and Continuity-Aware Delivery

Status: draft
Date: 2026-08-07

Defines how the queue surface (`ZQueueSocketBase`, 0002/0023) materializes
received messages: per message, whether the result is pooled or owned, and
how frame continuity is chosen. The low-level borrowed callback is a
separate, complete mode on the `OnFrame` member of `ZSocketBase`; this
document covers the materialization policy only.

## 1. Context

Receive has two tiers, aligned with 0002 and 0004:

- Low tier: the borrowed `OnFrame` surface delivers raw frames, zero copy, always.
  The caller owns everything it does with a frame during the callback.
- Queue tier: each peer's parser materializes messages directly into that
  peer's bounded receive queue, then the socket type aggregates the peer
  queues onto `Messages`.

Materialization follows 0004 constraint 1: the parser knows each frame length
from the header, so it rents the final pooled buffer (or allocates, for
owned) and reads into it directly. The "borrowed scratch then rent + copy"
shape is never the materialization path. A per-receive policy hook decides the
pooled/owned choice.

## 2. Goals

- Per-message choice of pooled or owned materialization via an optional
  `Decide` hook with a default mode.
- Frame continuity by policy: frames up to a limit are contiguous (single
  segment); larger frames may be segmented (off the LOH).
- Owned `byte[]` access that is zero-copy and explicit.
- Default behavior (the default `ZReceiveOptions` configuration) reproduces
  plain pooled materialization, segmented only above the contiguous limit.

## 3. Non-Goals (v1)

- Borrowed-tier segmentation: the low-level callback keeps receiving
  contiguous borrowed frames.
- Per-peer policy contexts: `Decide` carries message information only;
  connection identity is a later slice (ROUTER).
- Send-side policy changes.

## 4. Design

### 4.1 Modes and the Decide hook

```csharp
public enum ZReceiveMode
{
    Pooled, // rent a pooled buffer of exact frame length (default)
    Owned,  // GC.AllocateUninitializedArray; never touches a pool
}

public readonly struct ZReceiveAllocation
{
    public ZReceiveMode Mode { get; init; }
    public bool Segmented { get; init; }   // false = contiguous (default)
}

public readonly struct ZReceiveContext
{
    public int FrameLength { get; init; }
    public bool IsFirstFrame { get; init; }
    public bool HasMore { get; init; }
    public int FrameIndex { get; init; }
    public long AccumulatedLength { get; init; }
}

public interface IZReceivePolicy
{
    ZReceiveAllocation Decide(ZReceiveContext context);
}

public sealed class ZReceiveOptions : IZReceivePolicy
{
    public ZReceiveMode Mode { get; init; } = ZReceiveMode.Pooled;

    /// <summary>Frames longer than this materialize segmented; at or below, contiguous.</summary>
    public int ContiguousFrameLimit { get; init; } = 85_000;
}

public delegate ZReceiveAllocation ZDecide(ZReceiveContext context);
public sealed class ZDelegateReceivePolicy(ZDecide decide) : IZReceivePolicy;
```

`Decide` returns the allocation only - there is no rejection case, because
whether a frame may be received is not a per-frame policy concern (0008 D1):
the resource limits are connection-level options on `ZSocketOptions`
enforced by a guard outside the policy, and content-based filtering happens at
the message API, not in the allocator. `ZReceiveOptions` is the
configuration-only policy (fixed allocation); custom policies implement
`IZReceivePolicy` or wrap a `ZDecide` delegate via
`ZDelegateReceivePolicy`. The rejection contract, limits, and failure-class
separation are defined by 0008.

- Carried by `ZSocketOptions.ReceivePolicy` as a non-null
  `IZReceivePolicy` defaulting to `new ZReceiveOptions()` (0002), so the
  default behavior is the declarative default configuration; the low-tier
  `OnFrame` is not involved and stays borrowed.
- `Decide` is invoked **once per frame**, with the current frame's context
  plus message accumulation: `FrameIndex` is the zero-based index within the
  message and `AccumulatedLength` is the total bytes up to and including the
  current frame. Later frames see the accumulated decisions implicitly through
  these fields, so a message's frames may use different allocations (e.g. the
  first frame pooled, a later large frame owned).
- `ZReceiveOptions` is the configuration-only policy (fixed allocation);
  custom policies implement `IZReceivePolicy` or wrap a `ZDecide` delegate
  via `ZDelegateReceivePolicy`.
- The policy never rejects; resource violations reject the connection through
  the guard (0008 D6), and it is not a per-message drop. Consumers that want
  per-message filtering must do it at the message API, not in the allocator.
- `Borrowed` is not a mode here: borrowed delivery is the identity of the
  `OnFrame` tier, not an option of queue materialization.

### 4.2 Continuity policy

- Frames with length <= `ContiguousFrameLimit` materialize as a single
  segment (one rent, or one allocation for owned, of exactly the frame
  length; the parser reads into it directly, 0004 constraint 1).
- Frames above the limit materialize as chained segments of
  `SegmentBlockSize` (8,192, matching 0001) so large frames stay off the LOH.
  `ContiguousFrameLimit = 0` forces all frames segmented.
- Segmented materialization itself is not implemented yet (v1 materializes
  contiguously), but the decision surface is live: `Decide` and
  `ZReceiveOptions.ContiguousFrameLimit` already produce the `Segmented` flag.
- Segmenting a large frame requires reading it in blocks. Two options, to be
  resolved in review:
  - (a) The parser gains an optional block-read mode that hands out a frame as
    a chain of borrowed blocks; materialization then copies each block into
    its own pooled segment.
  - (b) Materialization reads through the borrowed contiguous frame and splits
    it into segments. Simpler, but doubles the copy for large frames.

### 4.3 Where materialization lives

Each peer's receive pump (the peer's parser + materializer) applies the
policy: for every frame it evaluates `Decide` with the frame's context and
message accumulation, materializes that frame into the peer's bounded receive
queue. The socket type later aggregates the peer queues; it does not
re-materialize.

### 4.4 Owned byte[] access

```csharp
public interface IZMessage
{
    // ...
    bool TryGetOwnedArray(int index, out byte[] array);
}
```

- Returns `true` and the backing array, zero-copy, only when frame `index` is
  owned (`Owner is byte[]`) and single-segment.
- Returns `false` for pooled frames and for segmented frames (owned or not).
- Throws `ObjectDisposedException` after `Dispose`, consistent with the other
  accessors.
- Contract: the returned array is the message's storage; callers must treat it
  as read-only while the message is alive, and should not retain it past
  `Dispose` (for owned frames `Dispose` is a no-op on the bytes, so retention
  is safe but discouraged).

## 5. Test Plan

- `Decide` returning `Owned`: queue messages are owned; a counting pool
  asserts zero outstanding rentals.
- Default configuration (`new ZReceiveOptions()`): plain pooled
  materialization, released on `Dispose`.
- `TryGetOwnedArray`: true for owned single frames (same instance as the
  source array); false for pooled and segmented; throws after `Dispose`.
- Zero extra copy: materialization rents the exact frame length and reads into
  it directly; the delivered buffer is the final buffer.
- Existing tests keep passing unchanged.

## 6. Open Questions

1. Is segmented materialization (4.2) in v1, and with which read strategy,
   (a) parser block-read or (b) split-after-read?
2. Does `TryGetOwnedArray` need a multi-segment owned variant (owned segments
   from distinct arrays), or is single-segment sufficient for the first cut?
