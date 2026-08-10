# 0004 - Per-Peer Queue Model and Performance Constraints

Status: draft
Date: 2026-08-07

Defines the queue model of the high-level socket (`ZQueueSocket<TSocket>`,
see 0002) and the hard performance constraints the implementation must
respect. The model follows libzmq: queues live per peer, not per socket;
socket types are selection policies over the per-peer queues.

## 1. Queue Model (per peer)

Each peer connection owns two bounded queues:

- Receive queue: `Channel<ZMessage>` with capacity = RCVHWM (BCL bounded
  channel with a library-owned `itemDropped` callback, so every message a
  drop mode discards is disposed). The peer's
  parser materializes messages directly into it. A slow peer fills only its
  own queue and pauses only its own parser.
- Send queue: `Channel<IZMessage>` with capacity = SNDHWM, drained by one
  send pump per peer that writes the connection. A slow peer's full send
  queue is handled per socket type (PUB drops, DEALER picks another peer,
  REQ blocks).

The socket type is a pure selection layer over these queues, matching libzmq:

| Type | Outbound selection | Inbound aggregation |
|---|---|---|
| PAIR | direct to the single peer | direct from the single peer |
| PUB | broadcast to all send queues | none (send-only) |
| SUB | none (receive-only) | fair-queue + subscription filter |
| REQ | round-robin; strict send/recv alternation | reply of the last request |
| REP | direct to the requesting peer; strict alternation | fair-queue |
| DEALER | round-robin (load balance) | fair-queue |
| ROUTER | routing-id -> specific send queue | fair-queue, sender id prefixed |
| PUSH | round-robin | none (send-only) |
| PULL | none (receive-only) | fair-queue |

Direct write (no send queue) remains an optimization for single-peer sockets
such as PAIR, where the queue adds a hop without buying isolation.

## 2. Hard Constraints

These are binding for every implementation slice. Violating one requires a
design review first.

1. Zero extra copy on materialization: the parser knows each frame length from
   the header, so it rents the final pooled buffer (or allocates, for owned)
   and reads into it directly. The delivered buffer is the final buffer. The
   "borrowed scratch then rent + copy" shape is never the final materialization
   path for the channel tier.
2. Per-peer bounded queues are the default for both send and receive;
   direct write is an optimization, never the primary model. The receive
   queue is a BCL bounded channel configured with the socket's full mode;
   drop modes dispose the dropped message through the channel's
   `itemDropped` callback, and explicit drains reuse the same disposal path.
3. Queue capacity equals the per-peer HWM; queues are bounded by default
   (0009). Peak memory is controlled by the queue limits, never by arrival
   rate alone. Drop modes keep peak memory bounded at the capacity instead
   of blocking the pump. An explicit `ZUnboundedQueueFactory` (0009) opts
   out of the per-peer bound for a queue.
4. Hot paths allocate at most one object per message (`ZMessage` /
   `ZMultiMessage`); frame tables are struct arrays; no LINQ, closures, or
   per-frame heap objects on the receive/send fast path.
5. Each peer queue has a single writer and a single reader (SPSC). Multiple
   producers must never write the same peer queue.
6. Avoid fine-grained awaits: coalesce reads and writes into fewer system
   calls; keep the per-peer pump loop tight.
7. LOH avoidance: frames up to the contiguous limit (85,000) stay pooled;
   larger frames are segmented so pooled blocks never fall into the LOH.

## 3. Performance Baseline

Reference points (loopback, small messages; rough, to be measured later):

- C libzmq: per-peer `ypipe` (lock-free SPSC) + shared io-thread poller.
  Zero-copy receive (refcounted buffer sharing). Latency ~5-20 us, throughput
  ~1-3M msg/s.
- NetMQ: libzmq port with the same io-thread/pipe architecture. It has a
  `BufferPool` and `Msg.InitPool`, but its common API (`ReceiveFrameBytes`
  and friends) returns `byte[]` by default, which copies or allocates;
  zero-allocation requires the caller to use `Msg` + `BufferPool` manually.
  Latency ~20-60 us, throughput ~0.3-1M msg/s.

With the per-peer model, ZmqSharp is expected to sit between the two:

- Latency: per-peer bounded queue + single pump, ~15-40 us; comparable to
  libzmq (both have one queue hop), and better than NetMQ's default `byte[]`
  path because our default path never materializes a second array.
- Throughput: zero-copy materialization + SPSC bounded channel, hundreds of
  thousands to ~1M msg/s on loopback; comparable to libzmq once the parser
  materializes in place.
- Average memory: bounded per-peer queues plus ArrayPool with immediate
  return; scales linearly with load. Expected to beat NetMQ's default path
  (transient `byte[]` under GC) and libzmq's large-message mmap latency.
- Peak memory: bounded by queue capacities (HWM); the constraints above make
  peak a configuration, not a load artifact.

## 4. Ceilings

Managed allocations, GC pauses, and await overhead are structural ceilings
that C libzmq does not have. The constraints keep per-message managed cost to
one object and zero copying; beating libzmq outright is not expected, but
matching it on latency/throughput while beating it on allocation behavior is
the realistic target.
