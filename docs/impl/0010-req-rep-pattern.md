# 0010 - REQ/REP Pattern Cores and Surfaces

Status: draft
Date: 2026-08-11

Designs the REQ and REP pattern cores and their public surfaces on top of the
0007 architecture (transport core, pattern core, semantic seam) and the
per-peer queue/aggregation primitives. Both patterns follow libzmq behavior:
strict send/receive alternation, round-robin REQ outbound, fair-queue REP
receive, directed REP reply routing, and the empty-delimiter framing on the
wire.

## 1. Wire semantics (empty delimiter)

libzmq REQ/REP frame a payload with an empty delimiter frame **in front of**
the payload, so a peer can distinguish request/reply payloads from routing
frames:

```text
REQ send:   [empty, payload frames...]
REP send:   [empty, reply frames...]
REP recv:   wire [empty, payload...]  -> interpret -> [payload...] request
REQ recv:   wire [empty, reply...]    -> interpret -> [reply...] reply
```

- `BuildOutbound(ZMessage)` prepends an empty frame (0007 M3: frames move from
  the semantic value into the wire message).
- `InterpretInbound(ZMessage)` removes the leading empty frame; a missing or
  non-empty first frame is a `ZeroMqProtocolException`.
- Frames move, never copy; the consumed semantic value is inert afterwards
  (0007 M3). The message passed to `RequestAsync` / `SendReplyAsync` is owned
  by the pattern once called.

## 2. REQ pattern core

State (guarded by a core lock): a round-robin cursor, `current` (the single
in-flight peer), and the pending reply task.

- **Send gate (strict alternation, libzmq EFSM)**: `RequestAsync` throws
  `InvalidOperationException` when a request is already in flight.
- **Round-robin outbound**: the cursor advances unconditionally before
  selection, so a slow or failing peer never starves the others; dead peers
  are skipped on the next call once retired. `current` is set atomically with
  the pending task creation.
- **Wait**: the returned `Task<ZMessage>` completes with the reply when the
  semantic seam delivers `current`'s message (per-peer pump serialization is
  the event loop: a new request is only routed after the previous reply
  arrives, because `current` is cleared on completion).
- **Seam**: a message from `current` is interpreted (delimiter stripped) and
  completes the pending task; a message from any other peer is discarded
  (0007 open question: out-of-order replies are not correlated).
- **Peer end**: when `current`'s connection ends, the pending task faults and
  `current` is cleared; the next `RequestAsync` round-robins on. A live-but-
  silent peer blocks the socket by design (no timeout, libzmq semantics).

`RouteOutbound` (the generic base send path) throws for REQ: sends go through
`RequestAsync` only.

## 3. REP pattern core

State: a cross-peer request slot (strict alternation).

- **Fair intake**: requests arrive through the semantic seam, one per peer,
  serialized per peer (0007 2.3). A single `SemaphoreSlim(1,1)` serializes
  across peers, so one request is handled at a time - strict alternation, no
  starvation (the per-peer pumps stay alive).
- **Directed reply**: `SendReplyAsync(context, reply)` sends
  `[reply..., empty]` back to `context.Peer` via the transport core's
  directed-send primitive. A reply to a retired peer is dropped (transport
  retirement semantics).
- **Request context** (`ZRequestContext`): the originating peer plus the
  interpreted request message. It is the sole owner of the message (0007 M2);
  the core disposes it after the handler completes, so the context is valid
  only for the handler call and must not be retained.
- **Handler**: `BindRequestHandler(Func<ZRequestContext, CancellationToken,
  ValueTask>)`; awaiting the handler holds the slot (backpressure on the
  seam). An unbound handler drops requests. A request that is not replied to
  is dropped when the handler returns.

## 4. Surfaces (composition roots)

```csharp
public sealed class ZReqSocket : ZSocketBase, IPatternSink
{
    public Task<ZMessage> RequestAsync(ZMessage message, CancellationToken token = default);
}

public sealed class ZRepSocket : ZSocketBase, IPatternSink
{
    public void BindRequestHandler(Func<ZRequestContext, CancellationToken, ValueTask> handler);
    public ValueTask SendReplyAsync(ZRequestContext context, ZMessage reply, CancellationToken token = default);
}
```

Each socket's protocol core consumes inbound messages before any surface
could see them, so a configured sink never receives and the raw `OnFrame`
surface is mutually exclusive with the pattern surface.

Construction: `new ZReqSocket(ZSocketOptions?)` / `new ZRepSocket(ZSocketOptions?)`
(no queue variants: REQ is operation-oriented, REP is a typed callback; 0022).

Socket-Type compatibility is extended: `REQ <-> REP`.

## 5. Ownership and lifecycle

- `RequestAsync`: the caller's message is consumed (framed and sent); the
  returned reply is owned by the caller and disposed once. A rejected call
  (in-flight, no peer) leaves the message with the caller.
- `ZRequestContext`: owned by the REP core; valid during the handler call;
  disposed by the core after the handler returns. `SendReplyAsync` consumes
  the reply; the context itself is disposed by the core.
- Counting-pool tests assert return to zero after each surface's disposal
  path (0007 M4).

## 6. Non-goals

- Request timeout or automatic peer failover (libzmq has neither by default).
- Correlation of out-of-order replies across several REP peers (0007 open
  question; that is DEALER/ROUTER territory).
- REP queue/channel surface (deferred; the typed callback is the primary
  shape per 0007 section 5).
