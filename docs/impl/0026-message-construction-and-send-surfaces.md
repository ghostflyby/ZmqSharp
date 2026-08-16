# 0026 - Public Message Construction and Send Surfaces

Status: accepted
Date: 2026-08-16
Revision: 1

Gives users a public way to build and send multipart messages. Today the
multi-frame construction path is entirely internal (`ZSegment`,
`ZSegments`, and `ZMultiMessage` constructors are `internal`), so a consumer
can send a single frame via `ZMessage.FromOwned(byte[])` but cannot send a
multipart message at all - a hard blocker for the Jupyter migration, whose
wire messages are `[identities..., HMAC, header, parent_header, metadata,
content]`. The design adds three byte-data inputs on both the construction
and send surfaces, maps them onto the internal single/multi ×
contiguous/non-contiguous × owned/pooled/borrowed matrix, and keeps the
internal matrix fully constructible without exposing segment semantics.

## 1. Problem

- **No public multi-frame construction.** `ZMessage.FromOwned(byte[])` is
  the only public factory (ZMessage.cs:38). `ZSegment(object, int, int)`,
  `ZSegments(ZSegment[])`, and `ZMultiMessage(ZFrame[])` are all internal,
  so no public expression of a multi-frame message exists.
- **Send surface mirrors the gap.** Every send-capable socket (PAIR, DEALER,
  PUSH, PUB, ROUTER identity overloads, REQ's `RequestAsync`, REP's
  `SendReplyAsync`) accepts `ZMessage` or a single `ReadOnlyMemory<byte>`
  (0024). There is no way to send several frames.
- **The internal matrix is already complete** (0005): segments are
  constructed from any owner (`byte[]` owned, `IMemoryOwner` pooled, parser
  scratch borrowed), frames are contiguous or non-contiguous, messages are
  single or multipart. What is missing is the *public* mapping from user
  byte data onto that matrix, and the send-surface wiring.

## 2. Design principle: three byte-data inputs

Users bring bytes in exactly three shapes; each maps onto a distinct message
form, and each form already exists internally:

| Input | Message form | Internal case |
|---|---|---|
| `ReadOnlyMemory<byte>` | single-frame message | `ZSingleMessage` + contiguous `ZSegment` |
| `IEnumerable<ReadOnlyMemory<byte>>` | multipart message, one frame per element | `ZMultiMessage` of contiguous frames |
| `ReadOnlySequence<byte>` | single frame with non-contiguous content | `ZSingleMessage` + `ZSegments` (one segment per memory in the sequence) |

The mapping is by *logical frame count*, not by input type: an
`IEnumerable` with one element is a single-frame message, `ReadOnlySequence`
is always one frame regardless of segment count. The fourth internal
combination - a multipart message whose frames are themselves
non-contiguous - is **not** expressible by any of the three inputs (it needs
a nested structure) and stays internal-only; the parser produces it and no
public input shape needs it. This asymmetry is deliberate and documented.

Ownership: all three inputs are borrowed caller data (no ownership transfer,
unlike `FromOwned(byte[])`), so construction **copies** into owned buffers.
The copy is the same `Pool.Rent` + copy path the send-side
`SendAsync(ReadOnlyMemory<byte>)` overload uses today (ZSocketBase.cs:487).

## 3. Public construction surface

### 3.1 Two faces: copy (`Copy`) and ownership transfer (`FromOwned` / `FromPooled`)

Zero copy requires transferring ownership, so the surface is split by what
the caller can hand over:

| Input | Face | Copy? |
|---|---|---|
| `byte[]` (single frame) | `FromOwned(byte[])` | zero copy |
| `byte[][]` (multipart, one array per frame) | `FromOwned(byte[][])` | zero copy |
| `IMemoryOwner<byte>` (single frame) | `FromPooled(IMemoryOwner<byte>)` | zero copy (pool-to-pool handoff) |
| `ReadOnlyMemory<byte>` / `ReadOnlySequence<byte>` / `IEnumerable<ROM>` | `Copy(...)` | copies (borrowed views: ownership cannot transfer) |

`ReadOnlyMemory<byte>` and `ReadOnlySequence<byte>` are views, not
ownable buffers - a span slice or a string segment has nothing to hand over -
so their face necessarily copies. `FromOwned`/`FromPooled` is the
zero-copy face for callers that own their buffers (the Jupyter case: wire
frames are serialization-produced `byte[]`).

### 3.2 `ZMessage`

```csharp
// Existing: zero-copy single frame, caller transfers ownership of the array.
public static ZMessage FromOwned(byte[] data);

// New: zero-copy multipart, one owned array per frame; the collection is
// stored, not copied (the arrays are caller-owned). Empty input throws.
public static ZMessage FromOwned(byte[][] frames);

// New: zero-copy single frame from a pooled buffer; the IMemoryOwner is
// handed over and disposed with the message.
public static ZMessage FromPooled(IMemoryOwner<byte> owner);

// New (copy face): single-frame message, copied into an owned buffer.
public static ZMessage Copy(ReadOnlyMemory<byte> data);

// New (copy face): multipart message, one owned frame per element. Empty
// input throws (a message has at least one frame). Eagerly enumerated at
// construction - the enumerable is never held across an await.
public static ZMessage Copy(IEnumerable<ReadOnlyMemory<byte>> frames);

// New (copy face): single frame with non-contiguous content; one owned
// segment per memory in the sequence. A single-segment sequence collapses
// to the contiguous form.
public static ZMessage Copy(ReadOnlySequence<byte> frame);
```

### 3.3 Case types mirror the same inputs

`ZSingleMessage` and `ZMultiMessage` are public structs (0005); each gains
factories so a case can be built directly and converted to `ZMessage` via
the existing implicit conversions:

```csharp
public readonly struct ZSingleMessage
{
    public static ZSingleMessage FromOwned(byte[] data);
    public static ZSingleMessage FromPooled(IMemoryOwner<byte> owner);
    public static ZSingleMessage Copy(ReadOnlyMemory<byte> data);
    public static ZSingleMessage Copy(ReadOnlySequence<byte> frame); // non-contiguous content
}

public readonly struct ZMultiMessage
{
    public static ZMultiMessage FromOwned(byte[][] frames);
    public static ZMultiMessage Copy(IEnumerable<ReadOnlyMemory<byte>> frames);
}
```

`ZMessage.Copy(...)` is defined as composition of the case factories; the
`FromOwned`/`FromPooled` family likewise - one implementation per case, no
duplicated logic at the root.

### 3.4 Why the split is explicit

`FromOwned` means "you transferred the buffer to me, zero copy";
`FromPooled` means "you transferred the pooled buffer to me, zero copy";
`Copy` means "these are borrowed views, I copy them". Naming the copy face
`Copy` - rather than `From`, whose .NET convention is neutral construction -
pins the allocation contract into the name, so the caller cannot mistake it
for a zero-copy path. A `byte[]` caller keeps using `FromOwned`; a
view-holder (span/ROM/sequence) has no transferable ownership and pays the
copy, which is unavoidable for borrowed data. `Copy` never silently steals
an array: it copies, so the caller's buffer stays usable - the safe default
for borrowed input.

### 3.5 Borrowed data cannot be a stored message member

Why a stored message must copy (or take ownership) is not a convention
choice - it follows from the lifetime contract async APIs place on borrowed
memory. In a normal async call the `ReadOnlyMemory<byte>` is a **call
parameter**: its lifetime is framed by the call. The caller pauses at its
own await and does not touch the buffer; on failure or cancellation the
reference lives on the stack, unwinds with the call, and the memory returns
to the caller naturally. The caller is the sole owner throughout, lending
the buffer out, and `await` returning means the borrow is over.

A message is the opposite: it is **storage**. `ZMessage` can be queued
(`SendQueueFactory`'s `Outbound` channel), held, or handed across threads;
once inside a queue, cancellation cannot pop and reclaim one arbitrary
member, and the socket's `Outbound` write (which returns as soon as the item
is dequeued by the send pump) gives no "my buffer is free again" point. A
borrowed message therefore has no safe point at which the caller's buffer
becomes free again, which is exactly the use-after-free the borrowed receive
path (parser scratch, `ZSegment.Borrowed`) avoids by confining its lifetime
to a synchronous callback - a confinement that is impossible for a stored
message.

Hence the two faces are forced, not chosen: `Copy` gives the message its own
buffer (the `PipeWriter.WriteAsync` pattern: copy now, the caller keeps its
buffer); `FromOwned`/`FromPooled` transfer ownership (a
`Channel<byte[]>`-style contract, but stricter: the library owns and
releases). A borrowed-and-stored message is precisely
`Channel<ReadOnlyMemory<byte>>`, where the caller must not mutate and the
library cannot dispose. Our message owns its disposal, so borrowed storage
would split ownership between caller (mutate) and library (dispose) -
impossible.

Borrowing therefore never appears on `ZMessage`. Its three construction
faces are all **storage shapes** - the message owns its memory (copy) or
takes ownership (transfer). A borrow is not a storage shape; it is a
**send-time lifetime contract** that only a socket API can hold: the awaited
send's completion guarantees the buffer is no longer in use, and the API
pins/holds the buffer until then. So even if a future socket surface gains a
borrowed overload (3.6), the borrow lives in that API's parameter contract -
`SendAsync(ReadOnlyMemory<byte> borrowed, ...)` - and never materializes as
a `ZMessage` case. The same principle is why borrowed segments exist only on
the receive side (`ZSegment.Borrowed`): the parser's callback confines the
borrow, an API contract again, not a message capability.

### 3.6 Borrowing feasibility across all send surfaces

Borrowing a caller buffer is feasible exactly when the **caller-observable
await completion guarantees the buffer is no longer in use**. How many
internal awaits the send path has is irrelevant - the caller sees one await,
and the BCL proves multi-stage async delivery (IOCP registration, completion
thread, deferred notification) does not block a borrow contract. What matters
is what the returned `Task`/`ValueTask` means. This design keeps a uniform
`Copy`/`FromOwned`/`FromPooled` surface everywhere, but the feasibility
differs per surface and is recorded here as the baseline for any future
borrowed overloads:

| Surface | Await completes when | Borrowing a caller buffer at this surface is |
|---|---|---|
| PAIR / DEALER / PUSH / PUB `SendAsync(message)` | message written & released (serial per-target await, then dispose) | **feasible** - contract equals the awaited send |
| ROUTER `SendAsync(identity, message)` | same, after identity resolution (unknown identity disposes, still awaited) | **feasible** |
| REQ `RequestAsync` | reply arrives, or request send faults (fire-and-forget send, reply-by-causality) | **feasible by causality** - reply only after the request left the socket |
| REP `SendReplyAsync` | directed reply written & released (explicit) | **feasible** - the strongest case |
| `SendQueueFactory.Outbound.WriteAsync` | item dequeued by the send pump (the send itself runs in the background) | **not feasible** - the buffer lives on after `WriteAsync` returns |

The one infeasible row is the outbound channel: it is a pure producer
surface whose await means "accepted into the queue", with no completion
signal tied to the send. Everything else on the public send surface already
waits for the write, so a borrowed overload would be implementable - the
library would pin/hold the caller's buffer until the awaited write
completes, exactly the BCL receive model. None of this is done in this
design: a borrowed overload is a one-off optimization (single caller
benefiting, no reuse in the steady state), it adds a lifetime contract the
rest of the API does not carry, and the copy face already serves the Jupyter
shape at equal correctness. The matrix exists so the decision is recorded,
not because a borrowed surface is planned. Where a row is feasible, the
borrow belongs to that socket API's parameter contract (3.5) - it never
becomes a `ZMessage` construction face.

### 3.7 No segment types leak

`ZSegment`, `ZSegments`, `ZFrame` construction stay as they are (segment types
remain construction-internal; `ZFrame` stays constructible only from internal
segment cases, per 0005 principle 5 - `ReadOnlyMemory` is never a frame case
because it carries no owner). The public construction surface
never names a segment type: `ReadOnlySequence<byte>` is the standard-library
expression of non-contiguous data, so users describe segmentation with BCL
types, not library-internal ones.

## 4. Internal construction surface (unchanged, confirmed complete)

The internal matrix already covers every combination and needs no new
constructors:

- **Owned**: `new ZSegment(byte[] owner, offset, length)` - caller-owned.
- **Pooled**: `new ZSegment(IMemoryOwner<byte> owner, offset, length)`.
- **Borrowed**: `ZSegment.Borrowed(scratchOwner, offset, length)` - parser
  scratch, `Dispose` no-op (0005 section 3).
- **Non-contiguous frame**: `new ZSegments(ZSegment[])`.
- **Multipart**: `new ZMultiMessage(ZFrame[])` / `new ZSingleMessage(ZFrame)`.

The new public `From` factories are implemented on top of these, so the
copy path reuses the same construction the parser uses - one construction
mechanism for both directions of the wire. The 0005 "future splitting"
escape hatch (inlining segment storage into `ZFrame`) is unaffected.

## 5. Send surface

Every send-capable surface gains the three-input set alongside its existing
`ZMessage` / `ReadOnlyMemory<byte>` overloads. Each overload constructs the
message via the corresponding `ZMessage.Copy` factory and forwards through
the existing protected `SendAsyncCore(ZMessage)` (0024), keeping the
socket-type-specific behavior (round-robin, broadcast, identity routing)
exactly as it is today. The zero-copy send path is
`ZMessage.FromOwned(...)` / `FromPooled(...)` followed by
`SendAsync(ZMessage)`: the message goes straight into the encoder with no
copy anywhere; the byte-input overloads are the copy convenience face.

### 5.1 Direct send types (PAIR, DEALER, PUSH, PUB)

```csharp
public ValueTask SendAsync(ZMessage message, CancellationToken token = default);                 // existing
public ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);       // existing
public ValueTask SendAsync(ReadOnlySequence<byte> frame, CancellationToken token = default);     // new
public ValueTask SendAsync(IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default); // new
```

### 5.2 ROUTER

```csharp
public ValueTask SendAsync(ReadOnlyMemory<byte> identity, ZMessage message, CancellationToken token = default);
public ValueTask SendAsync(ReadOnlyMemory<byte> identity, ReadOnlyMemory<byte> bytes, CancellationToken token = default);
public ValueTask SendAsync(ReadOnlyMemory<byte> identity, ReadOnlySequence<byte> frame, CancellationToken token = default);
public ValueTask SendAsync(ReadOnlyMemory<byte> identity, IEnumerable<ReadOnlyMemory<byte>> frames, CancellationToken token = default);
```

### 5.3 REQ and REP

`RequestAsync` and `SendReplyAsync(context, ...)` mirror the four-input set
(reply/request can be multipart; REQ's fair-queue selection and REP's
directed reply are untouched). PULL, SUB, and XSUB keep **no** send member
(0024) - the new overloads are added only to types that already send.

### 5.4 Rationale

- **Single source of truth**: every new overload is one line - construct via
  `ZMessage.Copy`, forward to `SendAsyncCore`/`SendToAsync`. No per-type
  message-assembly logic, so the mapping table in section 2 is enforced in
  exactly one place (the factories).
- **Jupyter shape**: a five-frame kernel message is
  `await client.SendAsync(new[] { hmac, header, parent, metadata, content }, token)`
  or `ZMessage.Copy` once and `SendAsync(message)`. The `ReadOnlySequence`
  variant covers single-frame multi-buffer sends (e.g. a segmented iopub
  payload) without a copy into one contiguous buffer first.
- **params convenience**: an `IEnumerable` overload accepts `byte[][]` /
  `ReadOnlyMemory<byte>[]` directly; a `params ReadOnlyMemory<byte>[]`
  overload is intentionally **not** added to the send surface because it
  collides with the existing single-argument `SendAsync(ReadOnlyMemory<byte>)`
  on the call `SendAsync(singleFrame)` (both forms are applicable). Callers
  pass an array literal instead, which is unambiguous and keeps the surface
  flat. The construction surface has the same reasoning: `Copy` takes
  `IEnumerable<ReadOnlyMemory<byte>>`, not a params overload.
- **Zero-frame rejection**: empty `IEnumerable` / empty `ReadOnlySequence`
  throw `ArgumentException` at construction - a ZMTP message has at least
  one frame, and silently sending an empty multipart message would be a
  protocol ambiguity.

## 6. Ownership and allocation contract

| Input | Face | Copy? | Result |
|---|---|---|---|
| `FromOwned(byte[])` | transfer | no (zero copy) | owned single frame |
| `FromOwned(byte[][])` | transfer | no (zero copy) | owned multipart, contiguous frames |
| `FromPooled(IMemoryOwner<byte>)` | transfer | no (pool handoff) | owned single frame, pooled buffer |
| `Copy(ReadOnlyMemory<byte>)` | borrowed view | one pool rent + copy | owned single frame |
| `Copy(IEnumerable<ROM>)` | borrowed views | frame table + one rent per frame | owned multipart, each frame contiguous |
| `Copy(ReadOnlySequence<byte>)` | borrowed view | segment table + one rent per memory | owned single frame, non-contiguous |

The receive hot path's zero-allocation guarantee (0006) is untouched: all of
this is on the send side, which already allocates (the existing bytes
overload rents). `From` allocates once per frame; `FromOwned`/`FromPooled`
are the zero-copy fast paths. AOT constraints hold - no reflection, no
dynamic generation; the enumerable is consumed with a plain foreach.

## 7. Test plan

- **Construction**: each `From`/`FromOwned`/`FromPooled` factory produces
  the expected case (`TryGetValue(out ZSingleMessage)` /
  `TryGetValue(out ZMultiMessage)`), the expected contiguity
  (single-segment sequence collapses to contiguous; multi-segment sequence
  yields `ZSegments` with per-memory segments), and owned buffers
  throughout. `FromOwned` and `FromPooled` are zero copy (pooling counters
  assert no extra rent in AllocationTests). Empty input throws.
- **Send**: for each direct type (PAIR and DEALER suffice; ROUTER with the
  identity overloads), send one frame via each of the three copy inputs and
  via `FromOwned`, and receive the identical bytes; send a five-frame
  message and verify frame count and per-frame content on the wire (Jupyter
  shape) through both `FromOwned(byte[][])` and `Copy(IEnumerable)`.
- **NetMQ interop**: a ZmqSharp DEALER sends a five-frame message to a
  NetMQ ROUTER; frame count and contents match. Round trip both directions.
- **REQ/REP and ROUTER**: multipart request/reply; ROUTER multipart with
  identity prefix.
- **Allocation**: `SendAsync(ReadOnlySequence<byte>)` on a single-segment
  sequence avoids the node-chain path (encoder `PrependHeader` single-segment
  branch); multi-segment accepts the transient chain (outside measured
  gates). `FromPooled` disposal returns the pool buffer exactly once.

## 8. Migration

Fully additive: `FromOwned(byte[])` and every existing `SendAsync` /
`RequestAsync` / `SendReplyAsync` signature stay byte-compatible. Existing
tests compile unchanged. The new factories and overloads are the only
additions; no internal constructor or the `ZSegment.Borrowed` path changes.
