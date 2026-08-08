# 0005 - Union-Like Value Types for Frames and Messages

Status: accepted
Date: 2026-08-08

Defines a value-type storage pattern that mimics the upcoming C# union
feature: storage is split per case, the active case is inferred from field
state instead of a real tag, and every case getter is a `TryGetValue` overload
that returns true only for its case. Frames and messages adopt this pattern so
the hot path stays allocation-free and the layout can migrate to real unions
later.

## 1. Motivation

The receive side inevitably produces two reference types - `IMemoryOwner<byte>`
for pooled buffers and `byte[]` for owned buffers. Earlier designs wrapped
these in `ZBufferRef` (internal), exposed `ZFrameSegments` (internal) for
segmented frames, distinguished message shapes through the `IZMessage`
interface over two classes, and leaked `TryGetContiguousFrame` /
`TryGetOwnedArray` accessors that did not match the case model. That surface
was too large, mixed concerns, and allocated per message.

The goal is one value-type shape per concept: split storage per case, infer
the case from field state (no real tag), expose every case through a
`TryGetValue` overload, and keep the message/frame surface minimal. Borrowed
frames are just segments with a no-op owner - no special frame shape for
callbacks.

## 2. Principles

1. Case and layout are orthogonal. A case (contiguous vs non-contiguous;
   single vs multipart) selects which storage is active. Ownership and segment
   count are properties of the stored segments, not cases.
2. The tag is compressed into field state. The active case is derived from
   which nullable field is non-null, never from a real tag field.
3. Every case getter is named `TryGetValue`, overloaded on the out parameter
   type; each returns true exactly for its case. There are no other
   `TryGet*` accessors. The container contracts (`Count`, `this[int]`,
   `GetEnumerator`, `Dispose`) keep their names.
4. All structs. Frames and messages are value types; every struct ships its
   own `Enumerator`.
5. Cases are public types. A union is constructed from exactly one case value;
   constructors take a single case parameter and no flags. `ReadOnlyMemory<byte>`
   is never a frame case because it carries no owner; `ZFrame` exposes
   `ToSequence()` for content access.
6. `IZMessage` retires: the single union-like message type expresses single
   vs multipart internally, so interfaces are not needed and there is no
   boxing.
7. The shape is symmetric across layers: a union root and both of its case
   types implement `IReadOnlyList` over the next level's element. Message
   layer elements are frames; frame layer elements are segments.

## 3. ZFrame (Contiguous / NonContiguous)

```csharp
public readonly struct ZFrame : IReadOnlyList<ZSegment>, IDisposable
{
    private readonly ZSegment? contiguous;     // Contiguous case
    private readonly ZSegments? nonContiguous; // NonContiguous case
    private readonly bool more;

    public ZFrame(ZSegment segment);
    public ZFrame(ZSegments segments);

    public bool More => more;

    public bool TryGetValue(out ZSegment segment);
    public bool TryGetValue(out ZSegments segments);

    public int Count { get; }
    public ZSegment this[int index] { get; }
    public Enumerator GetEnumerator();

    public ReadOnlySequence<byte> ToSequence();
    public void Dispose();
}
```

- `ZSegment` is the contiguous case: one buffer plus its ownership token. The
  owner is `byte[]` (owned), `IMemoryOwner<byte>` (pooled), or an internal
  no-op owner (borrowed). `GetOwnedArray(out byte[])` returns the backing array
  only for the owned case. It is itself `IReadOnlyList<ZSegment>` with a single
  element (itself), mirroring `ZSingleMessage`.
- `ZSegments` is the non-contiguous case: a table of segments, each with its
  own owner. It is `IReadOnlyList<ZSegment>`; `Dispose` releases every pooled
  segment.
- Borrowed frames carry a no-op owner, so the callback surface uses exactly the
  same type as materialized frames; there is no borrowed-specific frame shape.
- Contiguity is per frame, not per message: a multipart message may mix
  contiguous and non-contiguous frames freely.

`ZFrame` is therefore `IReadOnlyList<ZSegment>`: indexing and enumeration work
uniformly whether the frame is contiguous (one segment) or non-contiguous
(several), exactly as `ZMessage` is `IReadOnlyList<ZFrame>` across its single
and multipart cases.

## 4. ZMessage (Single / Multi)

```csharp
public readonly struct ZMessage : IReadOnlyList<ZFrame>, IDisposable
{
    private readonly ZSingleMessage? single; // Single case
    private readonly ZMultiMessage? multi;   // Multi case

    public ZMessage(ZSingleMessage single);
    public ZMessage(ZMultiMessage multi);

    public static ZMessage FromOwned(byte[] data);

    public bool TryGetValue(out ZSingleMessage single);
    public bool TryGetValue(out ZMultiMessage multi);

    public int Count { get; }
    public ZFrame this[int index] { get; }
    public Enumerator GetEnumerator();
    public void Dispose();
}
```

- `ZSingleMessage` holds exactly one frame; `ZMultiMessage` holds a frame
  table. Both are public structs implementing `IReadOnlyList<ZFrame>`.
- All three message types implement `IReadOnlyList<ZFrame>` with struct
  enumerators, so `foreach`, LINQ, and index access work uniformly.
- `FromOwned(byte[])` builds an owned single-frame message with zero copy;
  disposing it never touches a pool.
- Channels and the socket layer use `ZMessage` directly; no interface boxing.

## 5. Ownership and Contiguity Matrix

Every frame is one of:

| Frame case | Owner | Meaning |
| --- | --- | --- |
| Contiguous (`ZSegment`) | `byte[]` | owned, single segment |
| Contiguous (`ZSegment`) | `IMemoryOwner<byte>` | pooled, single segment |
| Contiguous (`ZSegment`) | no-op owner | borrowed view (callback) |
| NonContiguous (`ZSegments`) | per-segment owners | segmented frame |

Messages combine these freely: a `ZSingleMessage` may hold a contiguous or a
segmented frame; a `ZMultiMessage` holds any mix. Ownership is always read from each
segment's owner, never from the message or frame case; a pooled owner yields
its `byte[]` only when it is actually a `byte[]`.

## 6. Future Splitting and Migration

- `ZSegment` and `ZSegments` may be split further, or their fields inlined
  directly into `ZFrame`, so the segment table is owned without an
  intermediate struct.
- When C# unions land, the split-storage layout maps mechanically: each field
  group becomes a union case, the inferred tag becomes the real union tag, and
  `TryGetValue` overloads become union pattern matching.

## 7. Impact and Open Questions

- `Channel<ZMessage>` stores structs inline; no per-item heap allocation.
- Parser and encoder branch on `TryGetValue(out ZSegment/ZSegments)`; the
  borrowed path keeps zero-copy semantics through the no-op owner.
- Parser reads segment content through the `ZSegments` indexer; encoder reads
  each segment's `Memory`.
- Open: whether the `ZMultiMessage` frame table should be exposed as a span or stay
  hidden behind index-based access.

## 8. Test Plan

- `TryGetValue` overloads return true exactly for their case.
- Message and frame accessors behave identically for single/multipart and
  contiguous/non-contiguous inputs; `Count`, indexing, and enumeration stay
  uniform across `ZMessage` / `ZSingleMessage` / `ZMultiMessage`.
- Frame and segment accessors behave identically across `ZFrame` / `ZSegment` /
  `ZSegments`: `Count`, indexing, and enumeration over segments are uniform.
- `GetOwnedArray` returns `byte[]` only for owned single segments.
- `Dispose` is idempotent and returns pooled segments; counting-pool
  assertions keep passing.
