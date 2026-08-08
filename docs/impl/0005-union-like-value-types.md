# 0005 - Union-Like Value Types for Frames and Messages

Status: draft
Date: 2026-08-08

Defines a value-type storage pattern that mimics the upcoming C# union
feature: storage is split per case, the active case is inferred from field
state instead of a real tag, and every case getter is a `TryGetValue` overload
that returns true only for its case. Frames and messages adopt this pattern so
the hot path stays allocation-free, internal storage types are not exposed,
and the layout can migrate to real unions later.

## 1. Motivation

Today:

- `ZFrame` is a struct that mixes borrowed and materialized frames; its
  `Memory` returns the whole frame for single-segment frames but only the first
  segment for segmented frames, which is semantically awkward.
- `ZMessage` and `ZMultiMessage` are classes (one heap allocation per
  message), distinguished through the `IZMessage` interface.

The goal is one value-type shape per concept: split storage per case, infer
the case from field state (no real tag), expose every case through a
`TryGetValue` overload, keep internal storage types internal, and make frames
and messages structs so they no longer allocate on the heap.

## 2. Principles

1. Case and layout are orthogonal. A case (borrowed vs materialized; single
   vs multipart) selects which storage is active. Segment count and ownership
   are properties of the stored segments, not cases; internal types carry
   them.
2. The tag is compressed into field state. The active case is derived from
   which field is valid (a nullable reference being non-null, or the segment
   table having entries), never from a real tag field.
3. Every case getter is named `TryGetValue`, overloaded on the out parameter
   type. There are no other case-accessor names.
4. All structs. Frames and messages are value types; `IZMessage` retires
   because the single union-like message type expresses single vs multipart
   internally.
5. Internal types stay internal. `ZFrameSegments`, `ZBufferRef`, and friends
   are implementation details; public APIs expose only public types
   (`ReadOnlyMemory<byte>`, `byte[]`, `ReadOnlySequence<byte>`, ...). Internal
   case access may use `TryGetValue` overloads with internal out types.

## 3. ZFrame (Borrowed / Materialized)

```csharp
public readonly struct ZFrame
{
    private readonly ReadOnlyMemory<byte> borrowed; // borrowed case
    private readonly ZFrameSegments segments;       // materialized case
    private readonly bool more;

    public bool More => more;

    // Case inferred from field state, no real tag.
    private bool IsBorrowed => segments.Single is null && segments.Many is null;

    public bool TryGetValue(out ReadOnlyMemory<byte> borrowedView)
    {
        borrowedView = borrowed;
        return IsBorrowed;
    }

    internal bool TryGetValue(out ZFrameSegments segments)
    {
        segments = this.segments;
        return !IsBorrowed;
    }
}
```

- The public surface exposes only the borrowed case (`ReadOnlyMemory<byte>`).
- The materialized case is internal (`ZFrameSegments` is internal), used by
  the queue materializer; ownership stays on each segment's `ZBufferRef`.
- The awkward `Memory` property disappears; consumers pick the case through
  `TryGetValue`.

## 4. ZMessage (Single / Multipart)

`ZMessage` and `ZMultiMessage` merge into one struct with exactly two cases:
**Single** (one frame) and **Multi** (several frames). Each case value is
itself a union-like type: the Single case is a frame segment expression
(single segment or segment table), and the Multi case is a frame table whose
entries are themselves segment expressions.

```csharp
public readonly struct ZMessage : IDisposable
{
    // Case Single: one frame's segment expression.
    private readonly ZFrameSegments single;
    // Case Multi: frame table (each entry is a segment expression).
    private readonly ZFrameSegments[]? multi;
    private int disposed;

    public int Count => multi is null ? 1 : multi.Length;

    internal bool TryGetValue(out ZFrameSegments segments);      // Single case
    internal bool TryGetValue(out ReadOnlySpan<ZFrameSegments> frames); // Multi case

    public ReadOnlySequence<byte> this[int index] { get; }
    public bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory);
    public bool TryGetOwnedArray(int index, out byte[] array);
    public void Dispose();
}
```

- Tag compressed into `multi`: null = Single, non-null = Multi.
- Single-frame single-segment messages allocate nothing; storage is inline.
- Public accessors (`this[int]`, `TryGetContiguousFrame`, `TryGetOwnedArray`,
  `Dispose`) use only public types; internal case access goes through
  `TryGetValue` overloads with internal out types.
- `IZMessage` retires: it existed only to abstract the two classes, which the
  union-like type now expresses internally. The message surface is one value
  type; channels and the socket layer use it directly (no interface boxing).

## 5. Future Splitting and Migration

- `ZFrameSegments` (`Single` / `Many`) can later be split further, or its
  fields can be inlined directly into `ZFrame` / `ZMessage` so the segment
  table is owned without an intermediate internal struct.
- `ZBufferRef` (`Owner` + `Memory`) can likewise be split or inlined.
- When C# unions land, the split-storage layout maps mechanically: each field
  group becomes a union case, the inferred tag becomes the real union tag, and
  `TryGetValue` overloads become union pattern matching.

## 6. Impact and Open Questions

- Channel of `ZMessage` is now a struct: `Channel<ZMessage>` stores values
  inline (no per-item heap allocation).
- `ZFrameHandler` / sinks switch to `TryGetValue`; the borrowed path keeps
  zero-copy semantics.
- Open: whether the Multi case table is exposed as `ReadOnlySpan` / array
  internally or hidden behind index-based access only.
- Open: inlining `ZFrameSegments` fields into `ZFrame` / `ZMessage` now, or
  keeping the internal struct for readability until real unions arrive.

## 7. Test Plan

- Single-frame single-segment messages are value-typed (no heap allocation on
  the hot path).
- `TryGetValue` overloads return true exactly for their case.
- `ZMessage` single vs multipart accessors behave identically to the previous
  `ZMessage` / `ZMultiMessage`.
- `TryGetOwnedArray` still returns `byte[]` only for owned single segments.
- `Dispose` remains idempotent and returns pooled segments; counting pool
  assertions keep passing.
