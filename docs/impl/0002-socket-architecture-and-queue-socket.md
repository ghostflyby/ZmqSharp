# 0002 - Socket Architecture: Callback Primitive and Queue Socket

Status: draft
Date: 2026-08-07

Extends 0001 into the socket layer: a low-level callback primitive
(`IZCallbackSocket`) and the high-level queue socket
(`ZQueueSocket<TSocket>`) built on top of it, following the per-peer queue
model of 0004. The transport/connection separation matches the current
implementation.

## 1. Layering

```text
Application
  |
  +-- ZQueueSocket<TSocket>  high-level main API: takes over the low-level callback
  |                      at construction; per-peer queues; Channel delivery (0004)
  |
  +-- IZCallbackSocket   low-level primitive: bind/connect, peer management,
  |                      borrowed frame callback, direct send
  |
  +-- ZConnection        per-peer full-duplex session (internal)
  +-- ZmtpParser         per-peer frame parser (borrowed, see 0001)
  +-- IZTransport        pluggable bottom layer (connect / bind / accept)
```

Socket types are subtypes of a shared base (libzmq-style): each type is its
own class implementing its routing and aggregation semantics; the callback is
the low-level receive contract and stays independent of the queue tier.

## 2. IZSocket (Common Contract) and IZCallbackSocket

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
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask SendAsync(IZMessage message, CancellationToken token = default);
}

public interface IZCallbackSocket : IZSocket
{
    // Receive: borrowed streaming frame callback (low-level).
    event ZFrameHandler? OnFrame;
    event Action<Exception?>? PeerEnded;
    void ResumePaused();
}
```

- `IZSocket` is the small common contract (endpoints + direct send) shared by
  every socket surface.
- `IZCallbackSocket` adds the borrowed receive surface; `OnFrame` delivers each frame
  borrowed (valid only during the call); a
  multipart message arrives as consecutive frames until `More` is false.
  Returning false pauses the receive pump; `PeerEnded` reports connection
  teardown; `ResumePaused` resumes paused pumps.
- Send is direct and synchronous-with-ownership: the socket type's
  `RouteOutbound` selects the target connection(s) and the message is disposed
  after the last peer send.
- No queues on this interface; queue semantics live in `ZQueueSocket<TSocket>`.

## 3. ZQueueSocket (Queue Surface, Main Path)

```csharp
public sealed class ZQueueSocket<TSocket> : IZSocket
    where TSocket : ZSocketBase
{
    public ChannelReader<IZMessage> Messages { get; }       // aggregate reader over peer queues
    public ChannelWriter<IZMessage>? Outbound { get; }      // optional send channel
    public ValueTask SendAsync(IZMessage message, CancellationToken token = default);
    // Bind / Connect / Close forward to the wrapped TSocket.
}

public sealed class ZQueueSocketOptions
{
    public int ReceiveCapacity { get; init; }               // per-peer RCVHWM
    public int? SendCapacity { get; init; }                 // per-peer SNDHWM, optional
    public ZReceiveOptions? ReceivePolicy { get; init; }    // materialization, see 0003
}
```

- `ZQueueSocket<TSocket>` wraps a concrete socket type (`ZPairSocket`,
  `ZDealerSocket`, ...); the generic parameter carries the socket type and the
  wrapped socket is never exposed. Construction takes over the wrapped
  socket's per-peer frame delivery (SetFrameSink), so the two tiers are
  mutually exclusive by construction.
- Capacity is per peer (each peer gets its own bounded queue with the
  configured HWM), following 0004. `Messages` exposes the socket-level
  aggregated view selected by the socket type (fair-queue, direct, ...).
- When `SendCapacity` is set, producers write to `Outbound`; the socket
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

- Low level: `IZCallbackSocket.OnFrame` streams borrowed frames (0001 section 4).
- Delivery chain: the parser awaits an async sink (`IZMessageSink.OnFrameAsync`
  returning `ValueTask<bool>`); a pending task pauses that peer's pump until it
  completes (0007 section 6 step 2).
- Queue tier: each peer's parser materializes messages directly into its
  receive queue (zero extra copy, 0004 constraint 1), applying the receive
  policy (0003). The socket type aggregates the peer queues (fair-queue,
  direct, ...) onto `Messages`.
- Full mode: each peer's queue is a bounded channel whose full mode is
  `ZQueueSocketOptions.ReceiveFullMode` (`Wait`, `DropWrite`, `DropNewest`,
  `DropOldest`). Wait blocks on `WriteAsync` of the affected peer queue,
  pausing only that peer's parser; drop modes never block the pump and the
  dropped message is disposed by the library. Peer end and socket disposal
  explicitly drain every buffered message through the same dispose path (0006
  section 2.2/3.5).

## 7. Send Path

- Direct send on `IZSocket`: the socket type's `RouteOutbound` selects the
  connection(s), writes each selected connection, disposes the message after
  the last peer send.
- Queue tier: `Outbound` is bounded; the socket routes each message to the
  selected peers (direct write today; per-peer send queues with one pump per
  peer are 0004/D2).
- A message is written atomically per connection (single writer, never
  interleaved).

## 8. Socket Types

```csharp
public static class ZSocket
{
    // Queue surface (main path, short names).
    public static ZQueueSocket<ZPairSocket> CreatePair(ZQueueSocketOptions? options = null);
    public static ZQueueSocket<ZDealerSocket> CreateDealer(ZQueueSocketOptions? options = null);

    // Callback surface (suffixed).
    public static ZPairSocket CreatePairCallback(ZSocketOptions? options = null);
    public static ZDealerSocket CreateDealerCallback(ZSocketOptions? options = null);
}
```

Internally every type is a subtype of `ZSocketBase` overriding
`RouteOutbound` (libzmq-style `pair_t`, `dealer_t`, ...). v1:
`ZPairSocket`, `ZDealerSocket`. Later: `Router`, `Req`, `Rep`, `Pub`, `Sub`,
`Push`, `Pull`, each adding its own outbound selection and inbound
aggregation (0004 section 1 table).

The queue surface is the primary path with short factory names; the callback
surface is created through `*Callback` entries. Each factory constructs a
socket type subtype and, for the queue surface, wraps it in
`ZQueueSocket<TSocket>` (0004).

## 9. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D9 | `IZSocket` is the small common contract (endpoints + send); socket types are subtypes of `ZSocketBase` | libzmq structure: one subclass per socket type, shared mechanics in the base |
| D10 | Generic transport factory (`IZTransport<TSelf, TEndpoint>`) is the core | Transports plug in with typed endpoints and compile-time selection |
| D11 | `ZQueueSocket<TSocket>` is the high-level main API; it takes over the callback at construction | Matches 0001 D7/D8; two tiers are mutually exclusive by construction |
| D12 | Queue capacity is per peer (HWM per peer) | Matches libzmq; per-peer backpressure isolation (0004) |
| D13 | Connection sessions are internal; direct send is the low-level send path | Keeps the primitive small; queue semantics stay in the wrapper |
| D14 | String endpoints are a facade over the generic core | User-facing convenience without replacing the generic factory |

## 10. Test Plan

- TCP loopback round-trip through `IZCallbackSocket.OnFrame` (borrowed, multipart
  streamed) and through `ZQueueSocket<TSocket>.Messages` (assembled).
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
