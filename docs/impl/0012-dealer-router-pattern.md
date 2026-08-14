# 0012 - DEALER/ROUTER Pattern Cores

Status: draft
Date: 2026-08-11

Designs the DEALER/ROUTER pattern cores on the 0007 architecture, following
libzmq semantics: DEALER is asynchronous round-robin / fair-queue; ROUTER is
identity-aware routing where the first frame of each message is the routing
identity of the peer.

## 1. Wire semantics

libzmq ROUTER frames every message with the **peer's routing identity** as
the first frame. The identity is a transport-assigned routing id (a random
byte sequence) when the peer does not explicitly set one; the socket maps
inbound identities to its peer connections.

```text
ROUTER recv: [identity, payload frames...] -> interpret -> routed value (identity, payload)
ROUTER send: SendAsync(identity, payload)  -> outbound -> [identity, payload...]
```

- **Identity source**: ZMTP 3.0 uses the identity-frame metadata or a
  transport-assigned routing id. libzmq's ZMTP 3.x assigns a random 5-byte
  routing id to connections that do not advertise one. For this slice the
  identity is the connection reference materialized as a stable per-connection
  routing id (see 4).
- **Routing**: `SendAsync(identity, message)` looks up the connection by
  identity and sends the framed message to it via the directed-send
  primitive. An unknown identity drops the message (libzmq ROUTER default).
- **Inbound**: the transport core's semantic seam prefixes the peer's routing
  identity (a pattern hook) before delivering to the bound sink or the
  channel surface: the delivered message is `[identity, payload...]`.

## 2. DEALER pattern core

Already implemented in 0010's work: asynchronous round-robin outbound with an
unconditionally advancing cursor and fair-queue inbound via the channel
surface. This batch adds NetMQ interop tests for DEALER (both directions).

## 3. ROUTER pattern core

- `RouteOutbound` is not used for identity addressing; ROUTER sends go
  through `SendAsync(identity, message)` (or the `ReadOnlyMemory<byte>`
  overload), which resolves the identity to its peer and uses the directed
  send.
- The pattern core maintains a `Dictionary<IZConnection, byte[]>` mapping
  connections to routing identities, released on peer teardown.
- Consumers bind their own `IPatternSink` (or wrap the socket in the channel
  surface) to receive the identity-prefixed messages.

## 4. Identity materialization

ZMTP 3.0 does not transmit routing identities over the wire; libzmq assigns
each connection a local routing id. This slice assigns a stable per-connection
routing id on the peer's first inbound message (a monotonic 4-byte value
encoded big-endian via `BinaryPrimitives`), used both as the lookup key and
as the prefixed frame. The mapping is released when the peer ends. A peer
that advertises an explicit identity in its READY would use that (deferred:
RFC 23/37 routing-id metadata is a wire prerequisite).

## 5. Public shapes

```csharp
public sealed class ZRouterSocket : ZQueueSocketBase   // queue surface by default (0023)
{
    public ValueTask SendAsync(byte[] identity, ZMessage message, CancellationToken token);
    public ValueTask SendAsync(byte[] identity, ReadOnlyMemory<byte> bytes, CancellationToken token);
}

// Inbound: Messages delivers ZMessage; the first frame is the identity, the
// remainder the payload.
```

DEALER/ROUTER and REQ/REP/DEALER compatibility follow libzmq.

## 6. Interop acceptance

- DEALER: ZmqSharp DEALER <-> NetMQ DEALER both directions over TCP.
- ROUTER: ZmqSharp ROUTER <-> NetMQ DEALER (the router's own identity framing
  is exercised: the NetMQ peer sends bare payloads and receives replies
  addressed by the router-assigned identity).

## 7. Non-goals

- Explicit identity metadata in READY (RFC 23/37 routing-id).
- Identity-based filtering; ROUTER routes all messages.
