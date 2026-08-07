# 0002 - Socket, Connections, and Channel Bridge

Status: draft
Date: 2026-08-07

This document extends 0001 (message model + no-Pipe parser) into an
end-to-end usable library: a user-facing socket that binds or connects to
endpoints over any transport, manages one or more peer connections, and sends
and receives multipart messages through two-layer APIs (callback + optional
channel).

## 1. Architecture

The ZMQ socket model is followed exactly:

- `IZSocket` is the common contract shared by all socket types: bind/connect
  to endpoints over any transport, manage one or more peer connections, send
  and receive.
- Socket types differ ONLY in the message-to-peer scheduling policy:
  broadcast, rule-based dispatch, fair round-robin, and so on. Transport,
  connection, and messaging mechanics are shared and live in the base.
- Connections (ZMTP sessions) are internal to the socket; the transport is the
  pluggable bottom layer.

```text
Application
  |
  +-- IZSocket                contract: bind/connect, peers, send/receive (two layers)
  |     ^
  |   ZSocketBase             shared mechanics: endpoints, transports, connections, parser glue, dispatch
  |     ^
  |   socket types            differ only by IZSchedulingPolicy (broadcast / rule / fair / ...)
  |
  +-- ZConnection (internal)  per-peer session: byte source + parser + queue
  +-- Transport (pluggable)   TCP via IZByteSource, etc.
```

## 2. IZSocket (Common Contract)

```csharp
public interface IZSocket : IAsyncDisposable
{
    Task BindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task ConnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint, CancellationToken token = default)
        where TTransport : IZTransport<TTransport, TEndpoint>;
    Task CloseAsync(CancellationToken token = default);

    // Receive, two layers: callback (borrowed) + optional channel (owned).
    event ZBorrowedMessageHandler? OnMessage;
    ChannelReader<ZMessage>? Messages { get; }

    // Send, two layers: direct (ownership transfer) + optional send channel.
    ValueTask SendAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask SendAsync(ZMessage message, CancellationToken token = default);
    bool TrySend(ZMessage message);
    ChannelWriter<ZMessage>? Outbound { get; }
}
```

- The generic transport factory (`IZTransport<TSelf, TEndpoint>`) is the core
  contract: transports plug in with typed endpoints and compile-time selection
  (D10).
- String endpoints are a separate facade layer on top of the generic core, not
  a replacement (D14):

```csharp
public static class ZSocketExtensions
{
    // Parses "tcp://host:port" and dispatches to the matching generic transport.
    public static Task ConnectAsync(this IZSocket socket, string endpoint, CancellationToken token = default);
    public static Task BindAsync(this IZSocket socket, string endpoint, CancellationToken token = default);
}
```

- `Messages` / `Outbound` are non-null only when the socket was configured
  with channel capacities in `ZSocketOptions`; otherwise the socket is
  callback/direct only (D11).
- `OnMessage` delivers a borrowed `ZMessageView` (valid only during the event,
  0001 D4); the channel delivers owned `ZMessage` instances.

## 3. ZSocketBase

Implements all shared mechanics in `ZmqSharp.Sockets`:

- Endpoint parsing and transport resolution.
- Connection management: one `ZConnection` per peer; `ConnectAsync` spawns a
  connection, `BindAsync` accepts and spawns one per accepted peer.
- Parser integration: every connection runs `ZmtpParser`; its sink forwards
  frames into the socket's receive pipeline.
- Receive aggregation and send distribution, delegated to the scheduling
  policy.
- Backpressure: when the receive channel is full, pause connections and resume
  with hysteresis; with the optional send channel, producers backpressure
  naturally.
- `CloseAsync`: stop pumps, tear down connections, drain channels, dispose
  in-flight messages.

Socket types are created through a factory and differ only in policy:

```csharp
public static class ZSocket
{
    public static IZSocket Create(SocketType type, ZSocketOptions? options = null);
}
```

`SocketType` (v1): `Pair`, `Dealer` (fair dispatch). 0003 adds `Router`,
`Req`, `Rep`, `Pub`, `Sub`, `Push`, `Pull`.

## 4. Scheduling Policy (the only per-type difference)

```csharp
public interface IZSchedulingPolicy
{
    /// <summary>Select the outbound connection(s) for a message; empty = drop.</summary>
    IReadOnlyList<ZConnection> RouteOutbound(ZMessage message, IReadOnlyList<ZConnection> peers);

    /// <summary>Transform or drop an inbound message from a peer (e.g. ROUTER prepends routing id, SUB filters topic).</summary>
    ZMessage? OnInbound(ZMessage message, ZConnection peer);
}
```

v1 policies:

- `PairPolicy`: route to the single peer, drop when none; inbound unchanged.
- `FairPolicy`: outbound round-robin (load balance), inbound fair-queue
  (preserve arrival order across peers).

0003 policies: broadcast (PUB), rule dispatch by first frame (ROUTER), topic
filter (SUB), and the REQ/REP state machine.

## 5. ZConnection (Internal Session)

- Owns `IZByteSource` + `ZmtpParser` + a per-connection inbound queue.
- Runs the parse loop on its own task.
- EOF or protocol error tears the connection down and notifies the socket
  (reconnect is 0004).
- Supports pause/resume driven by socket-level backpressure.

## 6. Receive Pipeline (Two Layers)

- Callback: the socket raises `OnMessage` with a borrowed `ZMessageView`
  (0001 D4). Handlers must not retain the view.
- Channel: `Messages` is a bounded channel with capacity = HWM. When full, the
  socket pauses connections; a background resumer waits on
  `WaitToWriteAsync()` and resumes at `Count <= capacity / 2`. Protocol errors
  surface via `TryComplete(exception)`; close drains and disposes.

## 7. Send Path (Two Layers)

- Direct: `SendAsync(ReadOnlyMemory<byte>)` copies into an owned single-frame
  message; `SendAsync(ZMessage)` transfers ownership. The socket routes the
  message through the policy (possibly to several peers) and disposes it after
  the last peer send. `TrySend` is the non-blocking variant.
- Optional channel: `Outbound` is a bounded `ChannelWriter<ZMessage>`;
  producers `WriteAsync` (backpressure), and a send pump routes each message.
- Frame encoding: `ZmtpFrameEncoder` (reverse of the parser) writes
  flags + size + body per frame; messages are written atomically (never
  interleaved), with a single writer per connection.

## 8. Transport

- `IZTransport<TSelf, TEndpoint>` stays the pluggable bottom layer: a generic
  static factory; both `ConnectAsync` and `BindAsync` return the same transport
  type. Send and receive are unified in one type (`Stream?` when connected),
  and after `BindAsync` the same transport acts as a listener
  (`AcceptAsync` yields connected transports) - no separate listener type. The
  legacy `Send`/`Start`/`ZSocket` members are removed.
- The byte channel is the BCL `Stream`: the parser reads via
  `Stream.ReadAsync(Memory<byte>)` (chunking is caller-side: exact-size rents,
  8KB block chains, and the borrowed scratch all work unchanged), and the frame
  encoder writes via `Stream.WriteAsync`. `NetworkStream` over a TCP `Socket`
  is a single copy (kernel -> caller buffer), identical to direct
  `Socket.ReceiveAsync`; scatter/gather multi-buffer reads are the only
  Socket-only capability, deferred (unneeded for v1).
- The string facade resolves the scheme (`tcp://...`) to the matching generic
  transport; `ipc://` / `inproc://` are 0003.

## 9. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D9 | `IZSocket` is the common contract; socket types differ only by scheduling policy | ZMQ architecture: socket = pattern + routing + N connections |
| D10 | Generic transport factory (`IZTransport<TSelf, TEndpoint>`) is the core; typed endpoints, compile-time selection | Keeps the existing factory contract; transports plug in generically |
| D14 | String endpoints (`tcp://host:port`) are a separate facade over the generic core | User-facing convenience without replacing the generic factory; scheme dispatch lives in one layer |
| D11 | Receive and send each expose two layers: callback + optional channel | Matches 0001 D7/D8; the send channel is optional |
| D12 | TCP transport lands in this slice | Enables end-to-end loopback tests |
| D13 | Connection sessions are internal; reconnect deferred to 0004 | Keeps the slice focused; matches 0001 section 9 |

## 10. Test Plan

- TCP loopback: two sockets in-process, multipart round-trip, both receive
  layers.
- Multi-peer fair dispatch: one socket connected to two peers; verify
  round-robin send and fair-queue receive.
- Channel backpressure: full -> pause -> resume; counting pool asserts buffers
  are returned.
- Send ownership: message disposed after send; the copy path does not alias
  caller data.
- Lifecycle: endpoint parsing, bind/connect/disconnect/close, drain on close.
- Error propagation: a protocol error completes the receive channel.

## 11. Follow-ups

- 0003: full policy set (broadcast, rule dispatch, REQ/REP), plus `ipc://` and
  `inproc://` transports.
- 0004: reconnect with backoff, PING/PONG heartbeat, ERROR handling,
  PLAIN/CURVE security mechanisms.
