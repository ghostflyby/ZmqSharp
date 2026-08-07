# 0002 - Socket Architecture: Callback Primitive and Queue Socket

Status: draft
Date: 2026-08-07

Extends 0001 into the socket layer: a low-level callback primitive
(`IZSocket`) and the high-level queue socket (`ZQueueSocket`) built on top of
it, following the per-peer queue model of 0004. The transport/connection
separation matches the current implementation.

## 1. Layering

```text
Application
  |
  +-- ZQueueSocket       high-level main API: takes over the low-level callback
  |                      at construction; per-peer queues; Channel delivery (0004)
  |
  +-- IZSocket           low-level primitive: bind/connect, peer management,
  |                      borrowed frame callback, direct send
  |
  +-- ZConnection        per-peer full-duplex session (internal)
  +-- ZmtpParser         per-peer frame parser (borrowed, see 0001)
  +-- IZTransport        pluggable bottom layer (connect / bind / accept)
```

Socket types differ only in the selection policy over per-peer queues (0004
section 1). The callback is the low-level receive contract and stays
independent of the queue tier.

## 2. IZSocket (Low-Level Primitive)

```csharp
public interface IZSocket : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task CloseAsync(CancellationToken token = default);

    // Receive: borrowed streaming frame callback (low-level).
    event ZFrameHandler? OnFrame;

    // Send: direct, ownership transfers.
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask SendAsync(IZMessage message, CancellationToken token = default);
}
```

- `OnFrame` delivers each frame borrowed (valid only during the call); a
  multipart message arrives as consecutive frames until `More` is false.
  Returning false pauses the receive pump.
- Send is direct and synchronous-with-ownership: the socket routes through the
  policy and disposes the message after the last peer send.
- No queues on this interface; queue semantics live in `ZQueueSocket`.

## 3. ZQueueSocket (High-Level Main API)

```csharp
public sealed class ZQueueSocket : IAsyncDisposable
{
    public ZQueueSocket(IZSocket socket, ZQueueSocketOptions? options = null);

    public ChannelReader<IZMessage> Messages { get; }       // receive channel
    public ChannelWriter<IZMessage>? Outbound { get; }      // optional send channel
    public ValueTask SendAsync(IZMessage message, CancellationToken token = default);
    // Bind / Connect / Close forward to the wrapped IZSocket.
}

public sealed class ZQueueSocketOptions
{
    public int ReceiveCapacity { get; init; }               // per-peer RCVHWM
    public int? SendCapacity { get; init; }                 // per-peer SNDHWM, optional
    public ZReceiveOptions? ReceivePolicy { get; init; }    // materialization, see 0003
}
```

- Constructing `ZQueueSocket` takes over the wrapped socket's `OnFrame`
  callback, so a wrapped `IZSocket` never also delivers frames to a user
  handler: the two tiers are mutually exclusive by construction.
- Capacity is per peer (each peer gets its own bounded queue with the
  configured HWM), following 0004. `Messages` exposes the socket-level
  aggregated view selected by the socket type (fair-queue, direct, ...).
- When `SendCapacity` is set, producers write to `Outbound`; the socket
  routes each message into the selected peers' send queues.

## 4. Per-Peer Queue Model

The queue model is defined by 0004: each peer owns a bounded receive queue and
a bounded send queue; socket types are selection policies over these queues.
This document adopts that model without restating it.

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

- Low level: `IZSocket.OnFrame` streams borrowed frames (0001 section 4).
- Queue tier: each peer's parser materializes messages directly into its
  receive queue (zero extra copy, 0004 constraint 1), applying the receive
  policy (0003). The socket type aggregates the peer queues (fair-queue,
  direct, ...) onto `Messages`.
- Backpressure: a full receive queue pauses only that peer's parser; the
  socket type handles per-peer isolation (0004).

## 7. Send Path

- Direct send on `IZSocket`: routes through the selection policy, writes each
  selected connection, disposes the message after the last peer send.
- Queue tier: `Outbound` is bounded; the socket routes each message into the
  selected peers' send queues, drained by one pump per peer. A full send
  queue is handled per socket type (PUB drops, DEALER picks another peer).
- A message is written atomically per connection (single writer, never
  interleaved).

## 8. Socket Types

```csharp
public static class ZSocket
{
    public static IZSocket Create(ZSocketType type, ZSocketOptions? options = null);
    public static ZQueueSocket CreateQueue(ZSocketType type, ZQueueSocketOptions? options = null);
}
```

`ZSocketType` (v1): `Pair`, `Dealer`. Later: `Router`, `Req`, `Rep`, `Pub`,
`Sub`, `Push`, `Pull`. Each type maps to a selection policy (0004 section 1
table).

## 9. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D9 | `IZSocket` is the low-level primitive; socket types differ only by selection policy | ZMQ architecture: socket = pattern + routing + N connections |
| D10 | Generic transport factory (`IZTransport<TSelf, TEndpoint>`) is the core | Transports plug in with typed endpoints and compile-time selection |
| D11 | `ZQueueSocket` is the high-level main API; it takes over the callback at construction | Matches 0001 D7/D8; two tiers are mutually exclusive by construction |
| D12 | Queue capacity is per peer (HWM per peer) | Matches libzmq; per-peer backpressure isolation (0004) |
| D13 | Connection sessions are internal; direct send is the low-level send path | Keeps the primitive small; queue semantics stay in the wrapper |
| D14 | String endpoints are a facade over the generic core | User-facing convenience without replacing the generic factory |

## 10. Test Plan

- TCP loopback round-trip through `IZSocket.OnFrame` (borrowed, multipart
  streamed) and through `ZQueueSocket.Messages` (assembled).
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
- 0005: reconnect with backoff, heartbeat, ERROR handling, PLAIN/CURVE.
