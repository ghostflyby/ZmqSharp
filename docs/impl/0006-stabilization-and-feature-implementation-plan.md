# 0006 - Stabilization and Feature Implementation Plan

Status: draft
Date: 2026-08-09

This document defines the implementation order for stabilizing the existing
ZMTP, transport, message, and queue foundations before the public pattern APIs
are finalized. It records the constraints already accepted during design
review, separates confirmed repair work from open API decisions, and gives
each implementation slice an explicit completion gate.

This is a plan, not an acceptance of the still-open pattern API shapes,
exception hierarchy, or project license.

## 1. Outcomes

The plan targets these outcomes, in order:

1. Standards-correct ZMTP 3.0 framing, commands, NULL handshake, and metadata.
2. Explicit protocol resource limits and deterministic connection failure.
3. Value-type messages and frames whose storage access preserves the behavior
   of their existing underlying owners without adding a second lifetime model.
4. Fully asynchronous, bounded, per-peer queues with configurable wait and
   drop behavior and complete resource reclamation.
5. Allocation-stable peer snapshots and well-defined peer retirement.
6. An explicit, replaceable, AOT-compatible security mechanism boundary.
7. Pattern-specific public APIs designed from their semantics rather than
   forced into one channel reader/writer shape.
8. Interoperability coverage against libzmq through a CLRZMQ test adapter.
9. Documentation and packaging structure suitable for an eventual release.

## 2. Binding Constraints

The following constraints are approved and apply to every implementation
slice in this plan.

### 2.1 Value types and owner behavior

- `ZMessage`, `ZSingleMessage`, `ZMultiMessage`, `ZFrame`, `ZSegment`, and
  `ZSegments` remain value types.
- Do not introduce an internal reference type to add a shared lifetime,
  disposed flag, lease, reference count, or unified disposal state.
- Do not attempt to standardize behavior after `Dispose`. Repeated disposal
  and later storage access retain the behavior of the underlying owner.
- An owned `byte[]` remains accessible according to array semantics. A pooled
  `IMemoryOwner<byte>` may throw, return empty memory, return the original
  memory, or otherwise follow its own contract after disposal.
- Previously escaped `Memory<byte>`, `ReadOnlyMemory<byte>`, `Span<byte>`,
  `ReadOnlySpan<byte>`, and `byte[]` values are outside subsequent container
  disposal behavior.
- `ZSegment` may stop storing `Memory<byte>` directly. The preferred layout
  stores the existing owner or borrowed source plus an offset and length, then
  reacquires and slices the concrete memory on each content access.
- A path may deliberately access `IMemoryOwner<byte>.Memory` when observing
  the owner-specific post-disposal side effect is part of that path's
  contract. Shape-only operations such as `Count` and case discrimination do
  not gain artificial storage access solely to create uniform exceptions.
- ZmqSharp-owned disposal paths are designed not to throw for supported
  owners. Do not add exception aggregation, best-effort continuation, or
  compensation for hypothetical exceptions thrown by third-party or BCL
  `Dispose` implementations.
- Prevent known exceptions from non-disposal operations from bypassing
  required cleanup. Use ordering or `finally` where a known fallible operation
  precedes disposal, without adding handling for disposal exceptions.
- No null-forgiving operator, reflection, or dynamically generated code is
  introduced to implement the storage model.

### 2.2 Queue behavior

- Physical queues are bounded and per peer.
- Queue operations are asynchronous. Wait-mode backpressure awaits the
  affected peer queue and does not pause unrelated peers.
- Receive and send full modes are user-configurable. The initial mode set is
  `Wait`, `DropWrite`, `DropNewest`, and `DropOldest`, subject to naming review.
- Every bounded channel configured with a drop mode has a library-owned custom
  dropped-item operation. The operation disposes the exact item removed or
  rejected by the selected channel policy.
- The same drop operation is reused when unread buffered items are explicitly
  drained after completion, cancellation, peer retirement, pump failure, or
  socket disposal. Channel completion alone is not treated as reclamation.
- User diagnostics or drop hooks may observe a drop but cannot replace the
  mandatory disposal step. A known hook failure must not bypass disposal.
- Every dropped or abandoned owning message is disposed exactly through its
  existing value-type disposal path.
- Drop behavior is observable through diagnostics such as a counter or event;
  the final diagnostics API remains open.
- The synchronous borrowed callback tier remains separate from the owning
  asynchronous queue tier.

### 2.3 Connection semantics

- `ConnectAsync` completes successfully if and only if the complete ZMTP
  greeting, mechanism handshake, READY exchange, and required metadata
  validation have completed.
- TCP connection state is not a separate public socket state. TCP is one
  transport implementation behind `IZTransport`.
- Transport, greeting, mechanism, metadata, cancellation, and protocol
  failures fault or cancel the same `ConnectAsync` operation.
- Whether lower-level failures are wrapped in a ZmqSharp exception hierarchy
  or passed through is a separate API decision. The stabilization work must
  not silently convert failure into successful completion.

### 2.4 Pattern APIs

- PAIR and DEALER are early experiments, not precedents that freeze the final
  public API.
- A common channel reader/writer pair is not required as the public shape for
  every semantic socket.
- REQ may expose an operation that returns its reply. REP may expose a request
  context that owns reply routing. ROUTER may expose routing-aware send and
  receive values. These are design candidates, not accepted signatures; the
  shared architecture that fixes them (transport core, pattern core, semantic
  seam, composition roots, ownership/move rules) is designed in 0007, and its
  acceptance supersedes this candidate list.
- Queue types may remain internal implementation machinery even when a
  pattern exposes an async stream or operation-oriented API.

### 2.5 Security mechanisms and AOT

- The ZMTP security mechanism is explicitly replaceable through configured
  factories or instances. Discovery through reflection is prohibited.
- A mechanism boundary must cover greeting role, handshake commands,
  metadata, and traffic encoding/decoding. It is not limited to READY.
- NULL is the built-in first implementation. PLAIN and CURVE are later
  mechanism packages or implementation slices.
- `IsAotCompatible`, trimming, single-file, and AOT analyzers with warnings as
  errors are the required AOT checks. This plan does not add a Native AOT
  smoke application or publish test.

## 3. Confirmed Repair Work

### 3.1 ZMTP command and metadata compliance

The current implementation and tests encode command names as a name followed
by a NUL byte. ZMTP 3.0 encodes a command name as a one-byte length followed by
that many name bytes. READY metadata uses size-prefixed property names and
four-byte network-order property value lengths.

Required work:

- Add shared short-string command-name encoding and parsing.
- Encode and parse READY and ERROR according to RFC 23.
- Encode local `Socket-Type` metadata and parse peer metadata
  case-insensitively.
- Validate socket compatibility only where the semantic socket design defines
  the required relationship.
- Reject malformed command names, property names, lengths, and duplicate or
  invalid required metadata according to the selected validation policy.
- Reset or reuse command scratch storage so command-only traffic cannot grow
  retained scratch memory without bound.

Completion gate:

- Unit tests use standards-correct wire fixtures rather than fixtures that
  merely mirror the implementation.
- Correct READY and ERROR fixtures parse in partial-read configurations.
- Malformed command and metadata cases fail with protocol errors.
- At least one CLRZMQ/libzmq connection completes the NULL handshake in each
  direction.

### 3.2 Protocol resource limits

Add explicit options and checked accounting for:

- maximum frame size;
- maximum accumulated message size;
- maximum frames per message;
- maximum command size;
- greeting and handshake timeout;
- maximum concurrent incomplete handshakes, when listener ownership provides
  the required accounting point.

Limits are checked before renting, allocating, growing scratch storage, or
creating a segment table. Remote lengths must not escape as integer overflow,
array length, out-of-memory, or pool implementation exceptions when a
protocol-limit error is appropriate.

The receive-side limits (maximum frame size, maximum accumulated message size,
maximum frames per message) are implemented per 0008 Slice A with checked
accounting and terminal rejection. The remaining explicit options - maximum
command size (0008 Slice B), greeting and handshake timeout, and maximum
concurrent incomplete handshakes - are not yet implemented.

Completion gate:

- Boundary and one-past-boundary tests exist for every numeric limit.
- Multipart accumulation uses checked arithmetic.
- Oversized inputs fail before a corresponding large allocation or rent.
- A peer that violates a limit is removed and all of its accumulated owning
  storage is reclaimed.

### 3.3 Handshake completion and connection cleanup

Required work:

- Complete the establishment task successfully only after full ZMTP
  establishment.
- Complete it with the original or selected wrapped exception on failure.
- Ensure cancellation is reported as cancellation rather than success.
- Put parser, connection, peer-table, and establishment-gate cleanup in a
  failure-safe `finally` path.
- Prevent user callbacks from skipping internal cleanup when they throw.
- Ensure disconnect, listener shutdown, accept failure, and socket disposal do
  not leave a dead peer routable.

Completion gate:

- EOF, malformed greeting, ERROR, metadata rejection, transport failure, and
  cancellation all produce distinct non-success test outcomes.
- No failed connection remains in a routing snapshot.
- Callback exceptions cannot prevent parser and connection disposal.

### 3.4 Segment location storage

Refactor `ZSegment` without adding a reference-type lifetime abstraction:

- Owned storage retains the `byte[]` owner, a relative offset, and a length.
- Pooled storage retains the existing `IMemoryOwner<byte>`, a relative offset,
  and a length.
- Borrowed storage refers to the parser's existing scratch source without
  taking ownership of it, plus a relative offset and length.
- `Memory` and writable internal access reacquire the concrete memory from the
  array or owner and then slice it.
- `Dispose` continues to dispose only a pooled owner owned by that segment.
- Owner-specific behavior after disposal is neither caught nor normalized.

Tests must cover multiple underlying owner behaviors rather than asserting a
single universal post-disposal exception. Existing design document 0005 must
be amended or superseded where it specifies a no-op borrowed owner or universal
idempotence beyond the underlying owner contract.

Completion gate:

- `ZSegment` stores no `Memory<byte>` field.
- Owned, pooled, borrowed, empty, sliced, contiguous, and segmented cases
  retain their zero-copy behavior.
- Content access reacquires owner memory on every call.
- No new per-message or per-segment reference object is introduced.

### 3.5 Asynchronous per-peer queues

Replace synchronous `TryWrite` and global resume coordination in the owning
queue path with asynchronous per-peer operations.

Implemented:

- The delivery chain is async: `IZMessageSink.OnFrameAsync` /
  `ZFrameHandlerAsync` return `ValueTask<bool>`, the parser awaits the sink,
  and wait mode uses `WriteAsync` on the affected peer queue, pausing only
  that peer's pump (0007 section 6 step 2). The owning queue tier no longer
  depends on `ResumePaused`; the low-level borrowed callback API retains its
  own synchronous pause model.
- Receive full modes are configurable via `ZQueueSocketOptions.ReceiveQueueFactory`
  (`Wait`, `DropWrite`, `DropNewest`, `DropOldest`, 0009). Drop modes create the
  per-peer bounded channel with the BCL `itemDropped` callback wired to the
  library's mandatory `ZMessage.Dispose`, so the item selected by each mode is
  disposed exactly once and is never observed by the consumer.
- Explicit queue drains use the same dispose path, because channel completion
  does not invoke the dropped-item callback for buffered items: `OnPeerEnded`
  and socket disposal drain every peer's buffered receive messages, and socket
  disposal drains the outbound channel's buffered messages. A send-pump
  exception disposes the dequeued message before propagating.
- Waiting readers are woken on the 0->1 edge (`Reader.Count == 1` under the
  per-peer single writer), so a continuously readable queue does not produce a
  notification busy loop.
- Send-side (outbound) full modes are configurable via
  `ZQueueSocketOptions.SendQueueFactory` (`Wait`, `DropWrite`, `DropNewest`,
  `DropOldest`, 0009). The outbound channel is a BCL bounded channel with the same
  mandatory `itemDropped` disposal, so a drop mode never blocks a producer and
  every dropped message is reclaimed.
- A send-pump failure (peer failure, protocol error, closed socket) reclaims
  the dequeued message and completes the outbound channel with that failure,
  so producers discover it through a failing `WriteAsync` immediately instead
  of waiting for socket disposal. Cancellation and a producer-initiated
  channel completion exit the pump cleanly.
- Tests: wait mode loses no message under a full queue; a saturated peer does
  not pause another peer's delivery; each drop mode reports and reclaims the
  item selected by that mode; peer end and socket disposal return every
  buffered message to a counting pool; the outbound channel is drained on
  disposal; drop-mode outbound channels never block producers and reclaim
  every dropped message; a send-pump failure surfaces through the outbound
  channel before disposal.

Remaining required work:

- None under this section. Send-side full modes and send-pump failure
  propagation are implemented.

Completion gate:

- Saturation tests cover all full modes.
- Wait mode loses no message and does not pause another peer.
- Every dropped mode reports and disposes exactly the dropped message.
- Tests distinguish the incoming item from an existing newest or oldest item
  and prove that the custom callback receives the item selected by each mode.
- Completion and cancellation tests prove that buffered messages are reclaimed
  through the same drop operation even though no full-mode callback occurs.
- Counting-pool tests return to zero after peer failure, queue completion,
  cancellation, send failure, and socket disposal with unread items.
- A continuously readable queue does not produce a notification busy loop.

### 3.6 Peer snapshots and retirement

Implemented:

- Both hot paths read a copy-on-write snapshot as a single volatile load
  instead of allocating a list per operation: `ZSocketBase.peerSnapshot`
  (routable connections) and `ZQueueSocket.peerSnapshot` (active peer states)
  are rebuilt only when a peer is added or removed. `RouteOutbound` takes the
  snapshot as a `ReadOnlySpan<IZConnection>` and returns a single target
  (`IZConnection?`; null = drop), so the send path allocates no peer list and
  no result collection. Steady-state sends are allocation-free in an
  optimized build (0004 constraint 4; the absolute allocation gate is
  asserted under Release because Debug boxes async state machines).
- The send path routes to establishing peers and awaits their establishment
  gate (the failure still surfaces from `SendAsync`, per the decided
  semantics); the gate fast path skips the wait when the peer is already
  established. A peer that retires before or during its write is dropped
  (decided), not a fault, and never aborts the send.
- Peer receive-queue lifetime is modeled as `Active`, `Draining`, and
  `Closed` (`ZQueueSocket.PeerPhase`). On disconnect the peer is moved to
  `Draining`, removed from the aggregate snapshot, and reclaimed immediately
  (accumulated frames plus buffered messages disposed through the 0006 2.2
  path), then marked `Closed`; this satisfies the completion-gate
  counting-pool expectation that peer failure returns outstanding rentals to
  zero.
- The reclaim drain and the aggregate reader are serialized per peer by a
  `ReadLock`, because BCL `SingleReader` is a promise, not an enforced
  exclusion: a concurrent consumer read during the drain would otherwise be
  undefined behavior.
- Tests: `SendPath_NoPerMessageHeapAllocation` (Release gate, fake transport
  with an established peer), `ReceivePath_TryRead_NoPerMessageHeapAllocation`
  (Release gate, 1000 buffered messages drained through `Messages.TryRead`),
  and `PeerChurn_ConcurrentSendReadDispose_NoLeaksOrFaults` (concurrent send,
  drain, and connect/disconnect churn; failures asserted empty and the
  counting pool returns to zero after disposal).

Remaining required work:

- Pattern-specific fairness and routing rules (deferred until each pattern's
  API is designed); the generic snapshot primitive is free of starvation and
  per-operation allocations.
- Per-peer send queues (0004 D2), which the snapshot primitive already
  supports.

Completion gate:

- Hot-path allocation tests show no peer-list allocation per send or read
  attempt.
- Concurrent add, establish, disconnect, read, send, and dispose stress tests
  leave no unreachable queued messages or dead routable connection.

## 4. Replaceable Mechanism Boundary

Design the mechanism API before implementing PLAIN or CURVE. The design must
provide explicit, connection-scoped state without reflection-based discovery.

The boundary must be able to participate in:

- the mechanism name and greeting role;
- handshake command production and consumption;
- READY metadata production and validation;
- clear or protected traffic encoding and decoding;
- fatal mechanism errors and reconnect eligibility;
- client and server configuration.

The first implementation slice extracts current NULL behavior behind the new
boundary without changing its supported security level. A later design
document decides package boundaries, secret storage, authentication services,
and the exact PLAIN and CURVE APIs.

Completion gate:

- NULL uses only the mechanism boundary rather than parser special cases.
- A test mechanism can replace NULL through explicit configuration.
- No mechanism type is found through reflection, attributes, or dynamic code.
- Mechanism failures participate in the `ConnectAsync` failure contract.

## 5. CLRZMQ/libzmq Interoperability

CLRZMQ is introduced only in the test project as the managed bridge to native
libzmq. Native library installation and discovery must be explicit for Linux,
Windows, and macOS CI; local absence may skip a separately categorized
integration suite, but the release gate requires all configured CI jobs to run
it.

The initial matrix covers both directions for:

- greeting and NULL READY exchange;
- PAIR and DEALER where their current experimental semantics permit it;
- empty, short, long, and multipart messages;
- partial transport reads;
- peer close and protocol rejection;
- `Socket-Type` metadata and a selected incompatible pairing.

Self-roundtrip tests remain useful unit tests but do not count as standards
interoperability evidence.

## 6. Pattern API Design Track

Pattern work begins only after protocol, resource ownership, and per-peer queue
stabilization. Each pattern receives its own numbered design document before
its public API is implemented.

Recommended exploration order:

1. PAIR as the minimal single-peer lifecycle surface.
2. PUSH/PULL as one-directional load balancing and fair intake.
3. PUB/SUB as explicit lossy delivery plus subscription filtering.
4. REQ/REP as operation-oriented request/reply state machines.
5. DEALER/ROUTER as asynchronous routing and identity-aware delivery.

The design track must evaluate operation-oriented candidates such as a
reply-returning REQ send, a reply-capable REP request context, and explicit
ROUTER routing values. It must not preserve `IZSocket.SendAsync` or public
channel pairs solely for compatibility with the current prototype.

## 7. Documentation and Release Preparation

### 7.1 Design document organization

After this plan is reviewed, organize implementation documents as:

```text
docs/impl/
  README.md
  active/
  implemented/
  superseded/
```

Document numbers remain globally unique and are not reassigned when files
move. `docs/impl/README.md` records number, decision status, implementation
status, current path, and superseding document where applicable.

A document moves to `implemented/` only when its accepted behavior and tests
are complete. Partial implementations remain active. Moving existing files is
not part of creating this plan.

### 7.2 License decision

The project license remains open. Evaluate at least:

- Apache-2.0 for permissive adoption with an explicit patent grant;
- MIT for the simplest permissive terms;
- MPL-2.0 for file-level reciprocity while allowing proprietary applications.

Do not add a license expression or license file until the project owner makes
the selection. After selection, add the root license, package license
expression, repository metadata, package readme, copyright, and required
third-party notices. Test-only CLRZMQ/libzmq and assertion dependencies are
included in the notice and redistribution review.

### 7.3 Continuous integration

Add or retain these checks as the corresponding work lands:

- Release build with warnings as errors and compatibility analyzers;
- unit tests on Linux, Windows, and macOS;
- `dotnet format --verify-no-changes`;
- CLRZMQ/libzmq integration tests;
- queue saturation, cancellation, disconnect, and leak tests.

No Native AOT smoke publish is included.

## 8. Implementation Order

The recommended pull-request-sized sequence is:

1. Accept or revise this plan and reconcile affected statements in 0003-0005.
2. Implement the standards-correct command and metadata codec with wire
   fixtures and resource limits.
3. Correct `ConnectAsync` establishment failure and connection cleanup.
4. Add CLRZMQ/libzmq NULL-handshake and message interoperability tests.
5. Refactor `ZSegment` to owner/source plus offset and length.
6. Implement asynchronous per-peer receive queues and full-mode policies.
7. Complete send-queue failure propagation and all queue drain paths.
8. Replace per-operation peer-list copies with stable snapshots and peer
   retirement.
9. Extract NULL behind the replaceable mechanism boundary.
10. Write and review one design document per semantic socket family before
    implementing its public API.
11. Reorganize completed design documents and finish package/license metadata
    after the license decision.

Each slice includes focused tests and documentation updates. A later slice
must not compensate for a known ownership leak, swallowed establishment
failure, or non-compliant wire format left by an earlier slice.

## 9. Explicit Non-Goals of This Plan

- Introducing reference-type message, frame, segment, lease, or disposed-state
  wrappers.
- Defining uniform behavior for operations performed after disposal.
- Revoking already escaped memory, spans, sequences, or arrays.
- Finalizing all semantic socket APIs in this document.
- Selecting the project license without an explicit owner decision.
- Implementing PLAIN or CURVE before the mechanism boundary is accepted.
- Adding a Native AOT smoke application or publish test.
