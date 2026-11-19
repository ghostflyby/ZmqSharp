# 0033 - The Message Layer: A Three-Tier Transport/Protocol Model

Status: accepted (connection layering superseded by 0034)
Date: 2026-11-15

## 1. The existing model and where it broke

0030 fixed the transport seam at byte I/O: `IZConnection` is a byte pipe
(partial reads, zero-means-EOF, caller-supplied buffers), and everything
above - greeting, mechanism commands, traffic frames - is bytes on that pipe,
re-delimited by length prefixes the parser reconstructs. The defect analysis
(this document replaces several interface-first drafts; the accepted method
starts from the existing model, analyzes its defects, anchors on BCL
precedents, and derives the changed model):

- **D1 (L1)**: the byte contract encodes *pipe implementation* as
  *contract*: zero-means-EOF makes an empty message inexpressible,
  caller-supplied buffers cannot accept a transport-owned unit, and partial
  reads force unit-native transports to fake a stream.
- **D2 (L2)**: establishment assumes byte offsets - the greeting is "the
  first 64 bytes" and commands are stream-delimited.
- **D3 (L3)**: the encoder/parser re-derive delimiters the transport
  already has; on a delimiter-native transport the length prefixes are pure
  overhead and the materialization dance exists only to bridge caller
  buffers against wire buffers.
- **D4 (L0)**: `OnAccept` delivers only the byte connection, so a
  unit-native transport must impersonate a stream.

BCL anchors: `WebSocket`/`ValueWebSocketReceiveResult` (a unit read is a
result discriminant, never a byte count; a unit API never inherits
`Stream`), `ChannelReader<T>` (unit reads are ownership transfers),
`ConnectionContext`/`IConnectionListener` (one listener contract per
currency; protocols plug above a unified connection), and
`WebSocket.CreateFromStream` (framing is an adapter over a byte transport).
0030's rejection of BCL Pipelines stands: the byte contract keeps
caller-supplied buffers.

## 2. The changed model

Three layers, each a plug point. **The transport currency is the message**
(`ZMessage` - the library's own model, the atomic unit of ZeroMQ traffic:
the send transaction, the failure-discard unit, and the wire-atomic unit a
message-native transport offers); frame iteration is an implementation
detail of the framing:

```
L3  IZWireProtocol            establishment (greeting + mechanism, on the
                              connection's command primitives) + session
                              (send gate, message reads). Built-in:
                              ZmqProtocol. Plug: ZSocketOptions.WireProtocol.
    ZWireUnit                 one message + its kind (data, or command = a
                              single-frame control message; narrowed to
                              message + IsCommand in 0034)
L2  IZMessageConnection       ReadUnitAsync / WriteMessageAsync /
    IZMessageTransport<>      WriteCommandAsync. Native message sources plug
    IZFraming                 here; bytes-to-messages dialects plug here
                              (built-in ZmtpFraming: flag+length prefixes,
                              More-chain re-aggregation into messages).
L1  IZConnection (unchanged)  IZTransport (tcp/ipc, unchanged)
```

Compositions: tcp/ipc = L1 + ZmtpFraming + ZmqProtocol (the decomposed
original stack); an in-proc transport = L2-native + ZmqProtocol; ZWS is a
native L2 connection (one ZWS frame per WS message) plus a ZWS protocol
variant (0035), not a byte framing. `IZMessageConnection` is a *sibling* of
`IZConnection` - no unimplementable inherited members.

The runtime composes: a byte peer adapts its framing (command-bounded
during establishment, no policy allocator), runs the protocol's
establisher, then rebinds the framing reader in place (the codec is known
only after establishment) to the zero-copy allocator path (NULL codec) or
the codec-aware encoded bound. Everything above the receive loop
(socket-type checks, READY metadata, queues, materializers, lifecycle) is
shared and unchanged; the frame-level pump and the L4 aggregation both
dissolved - the framing aggregates, the session decodes, the runtime
routes whole messages.

**Boundary ownership**: delimiting AND aggregating are the framing's
business (L2); message semantics (command set, handshake, aggregation
rules) are the protocol's (L3); a framing sees the More flag and may
legitimately group a chain (e.g. one datagram per message). A command is
a single-frame control message - the establishment currency, surfaced
beside data messages rather than hidden inside the framing.

### The retention contract (send-side ownership)

`IZMessageConnection.RetainsMessages` declares what a completed write means
for the message's storage. Copying connections (framings, kernel-mediated
transports) consume the bytes during the call and return false - the sender
disposes. Retaining connections (zero-copy in-proc channels, proxies) hand
the frames onward and return true - ownership transfers on completion, the
sender must not touch or dispose the message, and the receiving chain
releases it; a thrown write never transferred ownership, so failures
dispose. The runtime honors this at the only point that sees a message's
full fan-out: a broadcast hands the original to the FIRST retaining target
and copies for each further one (each channel holds a unit only it will
release); directed and borrowed sends skip disposal when the session
retains. `IZWireSession.RetainsSentMessages` forwards the declaration so
protocol implementers surface it from their connection.

## 3. Existing API impact (0.9.0 breaking changes)

- **Removed** (absorbed, per the repo's retirement precedent - 0022
  factory retirement, 0030 surface cuts): `ZmtpParser`, `ZmtpFrameEncoder`
  (their substance lives in `ZmtpFraming` and `ZmqProtocol`), and the
  frame-currency intermediates from earlier drafts of this change
  (`IZFrameConnection`, `IZFrameTransport<>`, `ZWireFrame`, `IZWirePump`).
- **Added**: `IZMessageConnection`, `IZMessageTransport<>`, `IZFraming`/
  `ZmtpFraming`, `ZWireUnit`, `IZWireProtocol`/
  `IZWireSession`/`ZmqProtocol`, `ZFrameAllocator` (public), `IZSocket`/
  `ZSocketBase` `BindMessageAsync`/`ConnectMessageAsync`/`UnbindMessageAsync`/
  `DisconnectMessageAsync`, `ZSocketOptions.WireProtocol`, optional-
  `IZFraming` overloads on the existing `Bind/ConnectAsync` (the merge
  avoiding a parallel framing family).
- **Unchanged**: `IZByteReader`/`IZByteWriter`/`IZConnection`/`IZTransport`/
  `SocketTransport`, the message model, the mechanism four
  (`ZMechanismContext`'s public byte constructor adapts through the
  message layer internally), `IZFrameCodec` (a frame-body transform,
  orthogonal to the unit encoding), the string endpoint facade.

## 4. Performance invariants preserved

Enforced by the unchanged allocation gates: one logical write per frame
(FrameSequence gathering, iterated by the framing on send); the receive
path materializes frames directly into the receive policy's segments
(the allocator rides the framing reader, rebound after establishment) and
re-aggregates More chains with zero steady-state allocation; the
borrowed-tier copies ride a per-connection recycled-owner pool (handle
objects allocated once, returned on message dispose, released at
connection dispose - GC-free per frame); codec sends stream each staged
frame as one logical write through the framing fast path; codec reads
stage decoded frames in reused buffers and rebuild the message without
per-frame allocation (single-frame messages are struct-wrapped, no
allocation). The receive policy's limits are enforced by the allocator
itself during aggregation (connection-level, before allocation, 0008).

## 5. Tests

Behavior preservation: the full pre-existing suite (interop included) is
the decomposition's safety net; accounting assertions encode architecture
facts (scratch + recycled-owner lifetimes, in-flight delivery
convergence). New integration proofs (`FrameLayerIntegrationTests`)
exercise every plug point with real socket pairs: a native in-proc
message transport via `BindMessageAsync`/`ConnectMessageAsync`, a custom
4-byte length-prefix framing over the stock `SocketTransport` through the
`IZFraming` overload, and a custom greeting-less protocol through
`ZSocketOptions.WireProtocol`.
