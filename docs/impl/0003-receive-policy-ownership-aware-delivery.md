# 0003 - Receive Policy: Ownership- and Continuity-Aware Delivery

Status: draft
Date: 2026-08-07

Defines how the queue tier (`ZQueueSocket<TSocket>`, 0002) materializes
received messages: per message, whether the result is pooled or owned, and
how frame continuity is chosen. The low-level borrowed callback is a
separate, complete mode on `IZCallbackSocket`; this document covers the
materialization policy only.

## 1. Context

Receive has two tiers, aligned with 0002 and 0004:

- Low tier: `IZCallbackSocket.OnFrame` delivers borrowed frames, zero copy, always.
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
- Default behavior (`Decide == null`, default mode) reproduces plain pooled
  materialization.

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

public readonly struct ZReceiveContext
{
    public int FrameLength { get; init; }
    public bool IsFirstFrame { get; init; }
    public bool HasMore { get; init; }
}

public delegate ZReceiveMode ZDecide(ZReceiveContext context);

public sealed class ZReceiveOptions
{
    public ZReceiveMode DefaultMode { get; init; } = ZReceiveMode.Pooled;
    public int ContiguousFrameLimit { get; init; } = 85_000;
    public ZDecide? Decide { get; init; }
}
```

- Carried by `ZQueueSocketOptions.ReceivePolicy` (0002); the low-tier
  `IZCallbackSocket.OnFrame` is not involved and stays borrowed.
- `Decide` is invoked once per message, with the context of its first frame
  (`IsFirstFrame == true`, `HasMore` set from that frame). A message's mode is
  fixed for all of its frames; a mode split inside one multipart message is
  not supported.
- `Borrowed` is not a mode here: borrowed delivery is the identity of the
  `IZCallbackSocket` tier, not an option of queue materialization.

### 4.2 Continuity policy

- Frames with length <= `ContiguousFrameLimit` materialize as a single
  segment: one rent (or one allocation for owned) of exactly the frame
  length; the parser reads into it directly (0004 constraint 1).
- Frames above the limit materialize as chained segments of
  `SegmentBlockSize` (8,192, matching 0001) so large frames stay off the LOH.
  `ContiguousFrameLimit = 0` forces all frames segmented.
- Segmenting a large frame requires reading it in blocks. Two options, to be
  resolved in review:
  - (a) The parser gains an optional block-read mode that hands out a frame as
    a chain of borrowed blocks; materialization then copies each block into
    its own pooled segment.
  - (b) Materialization reads through the borrowed contiguous frame and splits
    it into segments. Simpler, but doubles the copy for large frames.

### 4.3 Where materialization lives

Each peer's receive pump (the peer's parser + materializer) applies the
policy: on the message's first frame it evaluates `Decide` (or the default
mode), then materializes every frame of that message into the peer's bounded
receive queue. The socket type later aggregates the peer queues; it does not
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
- `Decide` returning `Pooled` and `Decide == null`: plain pooled
  materialization, released on `Dispose`.
- `ContiguousFrameLimit`: frames at/below the limit are single-segment;
  `ContiguousFrameLimit = 0` produces segmented messages with correct
  `TryGetContiguousFrame == false` and correct sequence content.
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
