# 0023 - Queue-Default Surfaces and Callback Opt-Out

Status: accepted
Date: 2026-08-14
Revision: 1

Flips the surface relationship: the queue surface becomes the **default**
receive surface for every concrete socket that can deliver messages, and the
callback surface (raw `OnFrame`, or a custom `ZSocketOptions.MessageSink`)
becomes an explicit opt-out. The `ZQueueSocket<TSocket>` generic wrapper is
retired and its queue
machinery moves into a new `ZQueueSocketBase`; `ZQueueSocketOptions` is
absorbed into `ZSocketOptions`. Partially supersedes 0022 (queue surface is
now the default shape, not an explicit two-object composition).

## 1. Problem

0022 left the queue surface as an explicit two-object composition
(`new ZQueueSocket<T>(new T(), options)`), faithfully reflecting the two-tier
abstraction but not the expected usage: the queue surface is the primary
surface, the callback surface the minority, and the wrapper made the main path
the most verbose. The deeper issue was the generic wrapper itself - it had to
repeat the socket type (`ZQueueSocket<ZPairSocket>(new ZPairSocket())`), and
only four types (PAIR, DEALER, PULL, ROUTER) had queue variants, a shape
artifact rather than a design.

## 2. The flip

- Every non-protocol concrete socket (PAIR, DEALER, PUSH, PULL, ROUTER, PUB,
  SUB, XPUB, XSUB) now derives from `ZQueueSocketBase` and composes the queue
  surface **by default**: each peer's messages land in that peer's bounded
  queue and are read through `socket.Messages`. The send-only PUSH and PUB
  derive from the same base and compose an inert queue surface that never
  receives.
- `ZReceiveSurface.Callback` (new enum, `ZmqSharp` namespace) opts out: the
  socket composes no queue and the raw `OnFrame` surface is the delivery path.
  Only needed when no sink is configured: a custom
  `ZSocketOptions.MessageSink` implies callback semantics and makes this
  option redundant.
- REQ and REP stay on `ZSocketBase` (no `.Messages`): their protocol cores
  consume inbound messages before any surface could see them, so a queue
  surface would be dead configuration. `ReceiveSurface` is ignored on them.

```csharp
var pair = new ZPairSocket();                       // queue surface by default
await pair.BindAsync("tcp://*:5555");
var message = await pair.Messages.ReadAsync();      // reads per-peer queues

// Custom sink: configured at construction, no queue composed.
var raw = new ZPairSocket(new ZSocketOptions { MessageSink = mySink });

// Bare callback surface (raw frames): only needs ReceiveSurface.
var frames = new ZPairSocket(new ZSocketOptions { ReceiveSurface = ZReceiveSurface.Callback });
```

## 3. Machinery

`ZQueueSocketBase : ZSocketBase` owns the queue machinery that used to live in
`ZQueueSocket<TSocket>` (only three members referenced the generic parameter;
the rest were `ZSocketBase`-facing): per-peer `PeerState` queues, the
aggregate reader over the copy-on-write peer snapshot, edge-wake gating,
reclaim on peer end and disposal, the optional outbound channel and its send
pump, and the internal `QueueSurface` sink. The protected constructor has the
same shape as `ZSocketBase`'s composition face
(`(ZSocketOptions, IZDispatchPolicy, ZSocketType, IZInboundPolicy?)`), so
custom socket types choose their surface by base class: `ZSocketBase` for a
callback socket, `ZQueueSocketBase` for a queue socket.

At construction the queue surface wires `SetReceiveMaterialization` (the
allocation policy and the 0008 connection-level limits), binds its internal
sink, registers the per-peer connected handler, and starts the send pump. A
callback-surface socket wires none of these - it keeps the null policy, so
pass-through sockets run the borrowed frame tier and policy-composing sockets
(SUB, ROUTER, XPUB, XSUB) run the aggregated tier without a materializer,
preserving the pre-flip behavior exactly (this is why the materialization
config is composed by the queue base, not by `ZSocketBase`'s constructor).

The sink itself is constructor configuration: `ZSocketOptions.MessageSink`
(default null) is snapped by `ZSocketBase` at construction, and
`BindMessageSink` is now an internal seam used only by `ZQueueSocketBase` to
bind its own `QueueSurface`. A custom sink therefore implies callback
semantics and composes no queue (no materialization, no per-peer queues);
`ReceiveSurface` is only consulted when no sink is set. This closes the last
post-construction set-once seam - set-once is now entirely `init` properties
and constructors (0022's rule, fully realized).

`SetPeerConnectedHandler` became multicast: SUB's subscription propagation
(`SendSubscriptionsTo`) and the queue surface's per-peer state both register,
and both run on every new peer. The single-slot version could never have
hosted a queue-wrapped SUB; the flip makes the combination the default.

## 4. Options absorption

`ZQueueSocketOptions` is deleted; its six properties moved onto
`ZSocketOptions` alongside the existing five:

- `ZReceiveSurface ReceiveSurface` (new, default `Queue`)
- `IPatternSink? MessageSink` (new, default null): custom sink, delivered to
  directly; implies callback semantics and composes no queue
- `ZQueueFactory ReceiveQueueFactory` (default bounded 16 SPSC) - settable
  only on the queue surface
- `ZQueueFactory? SendQueueFactory` (default null)
- `IZReceivePolicy ReceivePolicy` (default `ZReceiveOptions`)
- `long MaxFrameLength` / `MaxMessageLength` (default `long.MaxValue`)
- `int MaxFramesPerMessage` (default `int.MaxValue`)

One configuration surface per socket: queue tuning is now set in the same bag
as the pool, security, and handshake limits. Queue configuration is
verifiable: the six options track whether they were explicitly set, and a
socket that composes no queue - a custom `MessageSink`, the callback surface,
REQ/REP, or a custom `ZSocketBase` subclass - throws at construction when any
was set, so silently-ignored configuration fails loudly. The guard lives in
`ZSocketBase`'s constructor behind the protected `ComposesQueueSurface`
virtual (overridden to true by `ZQueueSocketBase`), so every no-queue socket
is covered. The public getters keep their non-null, declarative defaults
(0008 D2); the explicit-set tracking is internal backing state.

## 5. Migration

`new ZQueueSocket<T>(new T())` → `new T()`; the queue options bag merges into
the socket's `ZSocketOptions`; a sink-bound socket sets
`MessageSink = sink` at construction (no `ReceiveSurface` needed), and a bare
frame socket adds `ReceiveSurface = ZReceiveSurface.Callback`. `.Messages` /
`.Outbound` / `.ReceiveRejections` read the same names directly on the socket.
All 309 tests were migrated without behavioral change; the receive allocation
tests keep measuring the pump-thread `IPatternSink` seam through a
constructor-configured sink.

## 6. Consequences

- The main path is `new ZPairSocket()` with `Messages` on the socket itself;
  the wrapper and its duplicated type parameter are gone.
- SUB, ROUTER, and XPUB gain the queue surface (subscriptions and per-peer
  queues coexist through the multicast handler).
- The 0022 rule stands: set-once is `init` / constructor; endpoints
  (`BindAsync` / `ConnectAsync`) are the only repeatable surface.
- There is no receive interface: `IZSocket` is endpoints + send only, and the
  callback surface is the borrowed `OnFrame` member of `ZSocketBase` itself.
  The retired `IZCallbackSocket` promised callback capability on the default
  queue surface where `OnFrame` throws and `ResumePaused` is a no-op; the
  concrete members say what each socket actually supports, so both surfaces
  are honest.
- Design documents 0001, 0002, 0004, 0006, 0007, 0008, 0009, 0011, 0012,
  0013, 0018, 0019 and the README were updated to the queue-default shapes.
