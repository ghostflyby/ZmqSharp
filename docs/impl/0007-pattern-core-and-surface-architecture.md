# 0007 - Pattern Core and Surface Architecture

Status: draft
Date: 2026-08-09
Revision: 2

Revision 2 changes:

- Fixes message ownership: every public boundary owns exactly one live copy of
  each piece of storage; internal transfers are moves, never copies. Semantic
  wrapper types exist only when they carry state that is not part of the
  message (REP request context, ROUTER routed value).
- Drops the `ZReply` wrapper: REQ is `Task<ZMessage> RequestAsync(ZMessage)`.
  `ZReply` added no state beyond the message and created a second disposal
  path, so it was removed.
- Splits the transport core from the pattern core. `ZSocketBase` becomes a
  pattern-agnostic transport core; pattern semantics live in per-type internal
  core objects composed with it, and `RouteOutbound` moves out of the base.
  Public types become thin composition roots (core x surface) instead of the
  `ZQueueSocket<TSocket>` generic wrapper.
- Simplifies the semantic seam to a single backpressure model: a
  `ValueTask` whose completion continues the peer pump, pending pauses it.
- Records the async delivery chain as an explicit implementation
  prerequisite (the current parser delivery path is synchronous).

Defines the shared architecture for pattern-specific public APIs: a
pattern-agnostic transport core, a per-pattern semantic core that owns wire
semantics, a single semantic delivery seam, and orthogonal delivery surfaces
(typed callback, channel, operation). It implements the API-shape direction of
0006 (section 2.4 and section 6) and generalizes the layering of 0001 and 0002
together with the per-peer queue model of 0004.

## 1. Problem

The base socket's `OnFrame` currently plays two roles at once: it is the public
low-level callback surface, and it is the internal delivery seam that the queue
wrapper takes over through `SetFrameSink`. That works while socket types are
"dumb" routing cores (`ZPairSocket`, `ZDealerSocket`). It stops working once
pattern semantics need to observe the frame stream:

- SUB must filter by topic before materializing the payload.
- REP must strip the empty delimiter and retain the originating peer to route
  the reply.
- ROUTER must expose the sender identity on every inbound message.
- REQ and REP must enforce strict send/receive alternation.

If both a pattern core and a channel wrapper consume raw frames, two consumers
fight over one seam. Borrowed-frame lifetime, per-peer serialization, and
backpressure propagation all require exactly one consumer at that level.

Two further structural problems drive this revision:

- `ZSocketBase` mixes transport mechanics with pattern hooks: `RouteOutbound`
  and `SocketTypeName` are abstract members of the base, so every socket type
  is a base subclass. Pattern state machines (REQ alternation, SUB filter,
  ROUTER identity) have no home except subclassing the transport.
- `ZQueueSocket<TSocket>` makes the delivery surface an inherent property of
  one public generic type. A channel reader/writer pair is a natural shape for
  flow-like patterns (PUSH, PULL, SUB inbound, PAIR) but not for
  operation-oriented patterns (REQ reply return, REP request context, ROUTER
  routing values). The concrete socket type functionality and the delivery
  surface must therefore be orthogonal and composable independently.

## 2. Design: three layers and one seam

```text
transport core (ZSocketBase, pattern-agnostic)
  per-peer connection pumps
    -> ZmtpParser raw frames
      -> wire seam (complete messages, per peer, serialized)
        pattern core (per type, internal)
          envelope interpretation / topic filter / state machine
          -> semantic seam (complete messages, per peer, serialized)
            surface (typed callback / channel / operation)
```

Exactly one surface is bound to the semantic seam per socket instance.

### 2.1 Transport core

`ZSocketBase` becomes the pattern-agnostic transport core. It retains the
shared mechanics: transport lifecycle, handshake, connection registry,
establishment gates, peer-end notification, and per-peer receive pumps that
materialize frames into complete messages (receive policy per 0003). It has no
pattern knowledge: no `RouteOutbound`, no `SocketTypeName`, no envelope
concepts.

The frame sink becomes internal. The public raw-frame `OnFrame` surface is
preserved only as the low-level surface of the raw socket type and is mutually
exclusive with every pattern-wrapped surface on the same instance.

The transport core exposes three primitives:

- `ValueTask OnWireMessageAsync(IZConnection peer, ZMessage message,
  CancellationToken token)` - the wire seam: complete messages, per-peer and
  serialized. Multipart aggregation is a transport-core responsibility; the
  pattern core never sees partial messages.
- `ValueTask SendToAsync(IZConnection peer, ZMessage message,
  CancellationToken token)` - directed send to a specific connection.
- Routable peer snapshots (0006 section 3.6) for the pattern core's outbound
  selection.

Receive-side materialization is wired by the transport core from the receive
policy; the pattern core is not involved in per-frame allocation decisions.
Topic filtering therefore operates on complete wire messages (the topic is the
first frame). Avoiding payload materialization for dropped topics is a
segmented-materialization optimization (0003 section 4.2), not a v1
requirement; see section 7.

### 2.2 Pattern core

Per-pattern internal objects composed with the transport core, not base
subclasses. A pattern core owns wire semantics and consumes the wire seam:

- `BuildOutbound(...)` - produces the wire message from a semantic value: REQ
  and REP append the empty delimiter frame, ROUTER prefixes the identity frame,
  SUB prefixes the topic. Move semantics, see section 3.
- `TryInterpretInbound(wireMessage, peer)` - produces the semantic message:
  REQ strips the trailing empty delimiter from the reply, REP strips it from
  the request, ROUTER peels the identity frame, SUB filters by topic (drop).
  Move semantics, see section 3.
- Send-path state machine transitions: REQ single in-flight alternation, REP
  reply alternation, SUB subscription set. These gates live in the core's send
  path, not in any surface, so every surface reuses the same state machine.
- Outbound selection: `RouteOutbound` moves from the base to the core, and the
  directed send `SendTo(peer, message)` covers REP reply routing and ROUTER
  identity addressing.

Per-peer receive state (frame index, accumulated length, message accumulator,
pattern state) lives in the core, mirroring today's `ZQueueSocket.PeerState`.

The pattern core emits complete semantic messages to the semantic seam.
Pattern receive state that is not part of the message - the ROUTER sender
identity and the REP originating peer - is per-connection routing metadata and
travels with the `peer` argument; surfaces present it (section 2.4).

### 2.3 Semantic seam

```csharp
public interface IPatternSink
{
    // A completed task continues this peer's delivery; a pending task pauses
    // the peer's pump until it completes. No separate resume operation.
    ValueTask OnMessageAsync(
        IZConnection peer,
        ZMessage message,
        CancellationToken token = default);
}
```

Rules of the seam:

- It carries complete messages with multipart preserved (0001 D3). All pattern
  interpretation happens in the core; surfaces perform no translation, only
  presentation of `(peer, message)` in their delivery shape.
- Invocation is per-peer and serialized: one message at a time on that peer's
  pump context.
- Backpressure is unified here: the returned `ValueTask`; surfaces decide
  their own policy (await user delegate, bounded-queue `WriteAsync`, pending
  operation matching).
- Ownership transfers to the surface: the surface disposes the message
  (mirrors the current channel path).

### 2.4 Surfaces

1. Raw callback surface (existing `IZCallbackSocket`): borrowed frames, valid
   only during the call. This is the low-level primitive and bypasses the
   pattern core entirely; an instance is either raw or pattern-wrapped, never
   both.
2. Typed callback surface: implements `IPatternSink`, presents `(peer,
   message)` as pattern-typed events (REP `ZRequestContext`, ROUTER routed
   value). Pattern delegates may be asynchronous (`ValueTask`); backpressure is
   their awaited completion.
3. Channel surface (evolution of `ZQueueSocket<TSocket>`): implements
   `IPatternSink`, writes per-peer bounded queues (0004), exposes the aggregate
   reader. Backpressure is the bounded channel's full state (pending
   `WriteAsync`), with the existing hysteresis resume.
4. Operation surface: implements `IPatternSink`, matches replies to pending
   operation tasks. REQ exposes `Task<ZMessage> RequestAsync(ZMessage)`; the
   REQ state machine guarantees one in-flight request, so a completed task is
   unambiguously the reply to the last request.

### 2.5 Orthogonality and composition roots

Three independent axes compose:

- Axis A: pattern core - wire semantics; varies per socket type.
- Axis B: surface - delivery shape; varies per public API mode.
- Axis C: materialization policy - borrowed / pooled / owned (0001 D6, D8;
  receive policy per 0003); wired by the transport core, belongs to neither
  pattern core nor surface.

Public types are thin composition roots that bind exactly one core and one
surface (section 5). There is no general `IZSocket.SendAsync` surface contract
and no `ZQueueSocket<TSocket>` generic wrapper: an instance of a concrete type
has exactly one set of members, so raw and pattern-wrapped modes are
mutually exclusive by type, not by convention.

## 3. Message ownership and move rules

This is the central ownership discipline of the architecture.

**Rule M1: every public boundary owns exactly one live copy of each piece of
storage.** A message value, once delivered across a boundary, is owned by the
receiver; the sender must not dispose, retain, or reuse it. The receiver
disposes it exactly once through the value-type disposal path (0006 section
2.1).

**Rule M2: semantic container types exist only when they carry state that is
not part of the message.** `ZMessage` is the single message currency across
every surface. REQ therefore returns `Task<ZMessage>`; `ZReply` was rejected
because it added no state beyond the message and would have created a second,
redundant disposal path. The only receive-side containers are:

- REP `ZRequestContext`: the originating peer reference plus the interpreted
  request message. It exists because the peer is routing state, not message
  content.
- ROUTER routed value: the sender identity plus the message. It exists because
  the identity is routing state.

A container is the sole owner of the message it presents: it exposes read-only
access and owns disposal, so "dispose the message separately from the
container" is impossible by shape.

**Rule M3: internal transfers are moves, never copies.** `ZSegment` storage is
a reference (the `byte[]` or `IMemoryOwner<byte>` owner); a value-type message
cannot be truly moved, so move is a calling convention enforced by API shape
and validated by counting-pool tests. The three transfers, each exactly once at
a seam:

- borrowed -> owned (materialization): the parser rents or allocates the final
  buffer and reads into it directly (0004 constraint 1).
- wire -> semantic (`TryInterpretInbound`): the case value is taken out of the
  wire message into the semantic value; the wire struct goes inert.
- semantic -> wire (`BuildOutbound`): frames are taken from the semantic value
  into the wire message (adding delimiter/identity/topic framing).

A consumed value must not be reused; surfaces never cache a moved-out message.

**Rule M4: dispose happens exactly once, by the last owner.** Each transfer
hands a single owner chain; a counting pool must return to zero after each
surface's disposal path. Double disposal is prevented by M1/M2 at public
boundaries and by M3 at internal seams - never by wrapper indirection.

## 4. Binding and lifecycle rules

1. One consumer per layer: the pattern core is the only consumer of the wire
   seam; one surface is bound to the semantic seam per instance.
2. Binding completes before any connection is established. The existing
   "set before connections, throw afterwards" enforcement of `SetFrameSink`
   is generalized to core and surface binding.
3. The seam carries complete semantic messages; the core performs all pattern
   interpretation; surfaces only present `(peer, message)`.
4. Backpressure is expressed on the seam (`ValueTask`); surfaces decide their
   own policy.
5. Send is not on the seam. Outbound goes through the core: `RouteOutbound`
   selection plus `BuildOutbound` framing plus directed send where the pattern
   requires it. `IZSocket.SendAsync` is not preserved as the pattern public
   shape (0006 section 2.4).
6. Send-path state machine gates live in the core: surfaces invoke core
   operations (`RequestAsync`, `SendReplyAsync`), and the core throws on
   illegal timing (send while a request is in flight, reply without a
   request). State machines are implemented once per pattern, never duplicated
   across surfaces.

## 5. Public surface shapes per pattern

| Type | Operation model (0004) | Suggested public surface |
|---|---|---|
| PAIR | symmetric, single peer | callback or channel |
| PUSH | send-only, round-robin | `SendAsync` only |
| PULL | receive-only, fair-queue | channel / async stream / callback |
| PUB | send-only, broadcast, topic prefix | `SendAsync(topic, payload)` |
| SUB | receive-only, topic filter | `Subscribe` / `Unsubscribe` + receive stream or callback |
| REQ | strict alternation, single in-flight | `Task<ZMessage> RequestAsync(ZMessage)` |
| REP | directed reply, strict alternation | `OnRequest(ZRequestContext)` or `ReceiveRequestAsync` + `SendReplyAsync(context, reply)` |
| DEALER | asynchronous round-robin / fair-queue | free-form send + receive stream or callback |
| ROUTER | identity-aware | `SendAsync(identity, message)`; receive returns a routed value |

The factory (`ZSocket.Create*`) selects the surface: `CreatePair` /
`CreatePairCallback`, and the analogous split per pattern as the surface set
lands. Each factory constructs the transport core, composes the pattern core,
binds the selected surface, and returns the concrete composition root. The
binding completes before any connection is established.

REQ's `RequestAsync` is the operation surface's view of the core: the core's
state-machine gate checks single in-flight, `BuildOutbound` adds the empty
delimiter, the transport core sends, and the semantic seam's reply message
completes the returned `Task<ZMessage>` with ownership moved to the caller.
REP's `ZRequestContext` holds the originating peer; `SendReplyAsync(context,
reply)` routes the reply back to that peer with a fresh delimiter and is
invalid after the reply is sent or the peer ends.

## 6. Evolution path from the current code

1. Make `ZSocketBase`'s frame sink internal and add the wire seam; move
   per-peer receive state (frame index, accumulator, materialization policy
   wiring) into the transport core. `OnFrame` survives only as the raw
   low-level surface.
2. Make the delivery chain async. Implemented: `IZMessageSink.OnFrameAsync`
   and `ZFrameHandlerAsync` return `ValueTask<bool>`, the parser awaits the
   sink, `SetFrameHandler` is the async seam, and the queue tier expresses
   wait-mode backpressure as `WriteAsync` on the affected peer queue. The
   frame-level seam is now awaitable; the semantic seam (`IPatternSink`,
   `ValueTask`) is the next surface-layer slice. The low-level borrowed
   callback tier keeps its synchronous `bool` + `ResumePaused` pause model.
   This is the implementation prerequisite for the whole architecture and
   lands with 0006 section 3.5 (async per-peer queues), before any
   seam-based surface does.
3. Extract pattern cores as internal composed objects and move
   `RouteOutbound` and `SocketTypeName` out of the base. `ZPairSocket` /
   `ZDealerSocket` become transport-core composition roots; the state machine
   cores (REQ/REP) land with their patterns.
4. Evolve `ZQueueSocket<TSocket>` into the channel surface bound to the
   semantic seam instead of `SetFrameSink`; keep per-peer queues, aggregate
   reading, and materialization.
5. Typed callback surfaces per pattern, in the 0006 section 6 exploration
   order (PAIR, PUSH/PULL, PUB/SUB, REQ/REP, DEALER/ROUTER).
6. Directed send lands with REP; the REQ operation surface lands with REQ;
   factory methods are extended alongside each pattern.

Each pattern receives its own numbered design document before implementation
(0006 section 6). This document fixes the shared architecture; pattern
documents fix exact signatures.

## 7. Open questions

- Seam naming, and whether `IPatternSink` (message level) should be renamed to
  avoid confusion with the existing frame-level `IZMessageSink`.
- Typed callback semantics: handler exceptions, serialization guarantees, and
  whether awaiting the handler pauses the peer pump for the whole pattern
  (natural for strict-alternation REP, unnecessary for flow surfaces).
- Whether the raw `OnFrame` surface remains a public factory path or moves to
  an explicitly advanced entry point.
- REQ multi-peer semantics: round-robin outbound plus strict alternation
  matches libzmq; out-of-order replies across several REP peers are not
  correlated (concurrent correlation is DEALER/ROUTER territory) and must be
  documented.
- Directed-send API shape and how REP maps an inbound request to its reply
  connection.
- SUB topic filtering on complete messages defers the "filter before
  materializing payload frames" optimization (0003 section 4.2); whether that
  is acceptable at scale or requires a per-frame observation hook in the
  transport core is deferred until segmented materialization exists.
- ROUTER identity: the ZMTP wire layer currently parses and produces only
  `Socket-Type` metadata; routing-id metadata (RFC 23/37) production and
  parsing is a wire prerequisite for the ROUTER pattern document.
- How candidate semantic values (`ZRequestContext`, ROUTER routed value) adopt
  the 0005 union-like shape so pattern documents inherit the pattern.

## 8. References

- 0001 - message model and API layering (borrowed vs owned, D1-D8).
- 0002 - socket architecture and queue socket (callback primitive, layering).
- 0004 - per-peer queue model (per-type selection table, hard constraints).
- 0005 - union-like value types (candidate shape for decision and routed
  values).
- 0006 - stabilization and feature implementation plan (sections 2.4 and 6:
  pattern API design track; section 3.5: async per-peer queues; section 2.1:
  value types and owner behavior).
