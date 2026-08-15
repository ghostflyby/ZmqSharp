# 0002 - Socket Architecture: Callback Surface and Queue Socket

Status: draft
Date: 2026-08-07

Extends 0001 into the socket layer: a low-level callback surface (the
borrowed `OnFrame` member of `ZSocketBase`) and the queue surface
(`ZQueueSocketBase`) that every deliverable socket composes by default (0023),
following the per-peer queue model of 0004. The transport/connection
separation matches the current implementation.

## 1. Layering

```text
Application
  |
  +-- ZQueueSocketBase      queue surface (default, 0023): per-peer queues; Channel delivery (0004)
  |
  +-- OnFrame (base)    callback surface (opt-out, 0023): borrowed frame callback,
  |                      direct send
  |
  +-- ZConnection        per-peer full-duplex session (internal)
  +-- ZmtpParser         per-peer frame parser (borrowed, see 0001)
  +-- IZTransport        pluggable bottom layer (connect / bind / accept)
```

Socket types are subtypes of a shared base (libzmq-style): each type is its
own class implementing its routing and aggregation semantics; the callback is
the low-level receive contract and stays independent of the queue tier.

## 2. IZSocket (Common Contract)

```csharp
public interface IZSocket : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint)
        where TTransport : IZTransport<TTransport, TEndpoint>;

    // Send: direct, ownership transfers.
    ValueTask SendAsync(ZMessage message, CancellationToken token = default);
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
}
```

- `IZSocket` is the small common contract (endpoints + direct send) shared by
  every socket surface; there is no receive interface - the callback surface
  is the borrowed `OnFrame` member of `ZSocketBase` itself (0023).
- `OnFrame` delivers each frame borrowed (valid only during the call); a
  multipart message arrives as consecutive frames until `More` is false.
  Returning false pauses the receive pump; `PeerEnded` reports connection
  teardown; `ResumePaused` resumes paused pumps.
- Send is direct and synchronous-with-ownership: the socket type's
  `RouteOutbound` selects the target connection(s) and the message is disposed
  after the last peer send.
- No queues on this interface; queue semantics live on the socket itself
  (`ZQueueSocketBase`, 0023).

## 3. Queue Surface (Default, 0023)

```csharp
public abstract class ZQueueSocketBase : ZSocketBase
{
    public ChannelReader<IZMessage> Messages { get; }       // aggregate reader over peer queues
    public ChannelWriter<IZMessage>? Outbound { get; }      // optional send channel
    // Send / Bind / Connect / Close from ZSocketBase.
}

// Options live on ZSocketOptions (one bag per socket, 0023):
public sealed class ZSocketOptions
{
    public ZReceiveSurface ReceiveSurface { get; init; } = ZReceiveSurface.Queue; // Callback opts out
    public ZQueueFactory ReceiveQueueFactory { get; init; } = new BoundedChannelOptions(16) { SingleWriter = true }; // per-peer queue (0009)
    public ZQueueFactory? SendQueueFactory { get; init; }     // optional outbound (0009)
    public IZReceivePolicy ReceivePolicy { get; init; } = new ZReceiveOptions(); // materialization, see 0003
}
```

- Every concrete socket that can deliver messages derives from
  `ZQueueSocketBase` and composes the queue surface by default (0023): the
  retired `ZQueueSocket<TSocket>` wrapper's machinery moved into the base.
  Construction binds the channel surface to the transport core's semantic seam
  (`IPatternSink`, 0007 section 2.3): the core aggregates complete messages
  and the surface writes them to the peer queues, so the two tiers are
  mutually exclusive by construction (a bound seam also rejects raw `OnFrame`
  subscription). `ZReceiveSurface.Callback` opts out: no queue is composed and
  the raw `OnFrame` surface is the delivery path; a custom
  `ZSocketOptions.MessageSink` implies callback semantics and is delivered to
  directly.
- Capacity is per peer (each peer gets its own bounded queue with the
  configured HWM), following 0004. `Messages` exposes the socket-level
  aggregated view selected by the socket type (fair-queue, direct, ...).
- When `SendQueueFactory` is set, producers write to `Outbound`; the socket
  routes each message to the selected peers (direct write today; per-peer
  send queues are 0004/D2).

## 4. Per-Peer Queue Model

The queue model is defined by 0004: each peer owns a bounded receive queue and
a bounded send queue; socket types implement outbound selection and inbound
aggregation over these queues. This document adopts that model without
restating it.

## 5. Transport and Connection Separation

As implemented:

- `IZTransport<TSelf, TEndpoint>`: generic static factory.
  `ConnectAsync(endpoint, options)` returns `IZConnection`;
  `BindAsync(endpoint, options)` returns a listener (`OnAccept` + `StartAsync`).
- `IZConnection`: full-duplex per-peer object; raw `ReadAsync`/`WriteAsync`
  for handshakes, frame/message send methods, and receive callbacks. No
  stream is exposed on the interface, and no handshake is built in: the
  driver composes Send and Receive so security mechanisms can vary.
- `SocketTransport`: TCP; `ConnectAsync` yields a connection,
  `BindAsync` yields a listening transport that reports accepted peers via
  `OnAccept`. IPC (Unix domain sockets) plugs in the same way with its own
  endpoint type.
- String endpoints (`tcp://host:port`) are a facade over the generic core.

## 6. Receive Pipeline

- Low level: the borrowed `OnFrame` surface streams raw frames (0001 section 4).
- Delivery chain: the parser awaits an async sink (`IZMessageSink.OnFrameAsync`
  returning `ValueTask<bool>`); a pending task pauses that peer's pump until it
  completes (0007 section 6 step 2).
- Semantic seam: the transport core aggregates each peer's frames into
  complete messages and delivers them through `IPatternSink.OnMessageAsync`
  (per peer, serialized); the queue surface is one such sink (0007 section
  2.3/6 step 1+4).
- Queue tier: the surface materializes each message into its peer's receive
  queue (zero extra copy, 0004 constraint 1), applying the receive policy
  (0003). The socket type aggregates the peer queues (fair-queue, direct, ...)
  onto `Messages`.
- Full mode: each peer's queue is built by `ReceiveQueueFactory` (0009); a
  bounded factory's full mode is `Wait`, `DropWrite`, `DropNewest`, or
  `DropOldest`. Wait blocks on `WriteAsync` of the affected peer queue,
  pausing only that peer's parser; drop modes never block the pump and the
  dropped message is disposed by the library. Peer end and socket disposal
  explicitly drain every buffered message through the same dispose path (0006
  section 2.2/3.5).

## 7. Send Path

- Direct send on `IZSocket`: the socket type's `RouteOutbound` selects the
  connection(s), writes each selected connection, disposes the message after
  the last peer send.
- Queue tier: `Outbound` is bounded when `SendQueueFactory` builds a bounded
  channel; the socket routes each message to the
  selected peers (direct write today; per-peer send queues with one pump per
  peer are 0004/D2). The outbound channel's full mode comes from the factory
  (0009); a drop mode never blocks a producer and the dropped message is
  disposed by the library. When the send pump fails, the channel completes
  with that failure so producers discover it through a failing `WriteAsync`
  immediately rather than at socket disposal (0006 section 3.5).
- A message is written atomically per connection (single writer, never
  interleaved).

## 8. Socket Types

```csharp
// Queue surface is the default (0023): the socket is its own queue surface.
var pair = new ZPairSocket();
var dealer = new ZDealerSocket();

// Callback surface: explicit opt-out on the same composition root.
var pairCallback = new ZPairSocket(new ZSocketOptions { ReceiveSurface = ZReceiveSurface.Callback });
var dealerCallback = new ZDealerSocket(new ZSocketOptions { ReceiveSurface = ZReceiveSurface.Callback });
```

Internally every type is a subtype of `ZSocketBase` overriding
`RouteOutbound` (libzmq-style `pair_t`, `dealer_t`, ...). v1:
`ZPairSocket`, `ZDealerSocket`. Later: `Router`, `Req`, `Rep`, `Pub`, `Sub`,
`Push`, `Pull`, each adding its own outbound selection and inbound
aggregation (0004 section 1 table).

The queue surface is the default receive surface of the socket itself
(`ZQueueSocketBase`, 0023); the callback surface is an explicit opt-out on
the same composition root. Construction is direct (0022, 0023): set-once
configuration lives in `ZSocketOptions` as `init` properties, and endpoint
binding/connection is the only repeatable surface
(`BindAsync` / `ConnectAsync`, repeatable).

## 9. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D9 | `IZSocket` is the small common contract (endpoints + send); socket types are subtypes of `ZSocketBase` | libzmq structure: one subclass per socket type, shared mechanics in the base |
| D10 | Generic transport factory (`IZTransport<TSelf, TEndpoint>`) is the core | Transports plug in with typed endpoints and compile-time selection |
| D11 | The queue surface is the default receive surface, owned by `ZQueueSocketBase` (0023); the callback surface is an explicit opt-out | Matches 0001 D7/D8; the two tiers are mutually exclusive by construction |
| D12 | Queue capacity is per peer (HWM per peer) | Matches libzmq; per-peer backpressure isolation (0004) |
| D13 | Connection sessions are internal; direct send is the low-level send path | Keeps the primitive small; queue semantics live in the base |
| D14 | String endpoints are a facade over the generic core | User-facing convenience without replacing the generic factory |

## 10. Test Plan

- TCP loopback round-trip through the borrowed `OnFrame` surface (multipart
  streamed) and through `Messages` on the default queue surface (assembled).
- Per-peer isolation: one slow peer must not pause another peer's receive
  queue.
- Channel backpressure: full -> pause that peer -> resume with hysteresis;
  counting pool asserts buffers are returned.
- Send ownership: message disposed after direct send; the copy path does not
  alias caller data.
- Lifecycle: bind/connect/disconnect/close, drain on close.
- Error propagation: a protocol error completes the affected peer's receive
  queue.

## 11. Follow-ups

- 0003: receive policy (pooled/owned, Decide, continuity, owned `byte[]`).
- 0004: per-peer queue model and performance constraints.
- 0006: reconnect with backoff, heartbeat, ERROR handling, PLAIN/CURVE.
