# 0033 - The Frame Layer: A Three-Tier Transport/Protocol Model

Status: accepted
Date: 2026-11-15

## 1. The existing model and where it broke

0030 fixed the transport seam at byte I/O: `IZConnection` is a byte pipe
(partial reads, zero-means-EOF, caller-supplied buffers), and everything
above - greeting, mechanism commands, traffic frames - is bytes on that pipe,
re-delimited by length prefixes the parser reconstructs. The model analysis
(this document replaced three earlier interface-first drafts; the method
follows the user's directive to start from the existing model, analyze its
defects, and derive the changed model, anchored in BCL precedents):

- **D1 (L1)**: the byte contract encodes *pipe implementation* as *contract*:
  zero-means-EOF makes an empty message inexpressible, caller-supplied
  buffers cannot accept a transport-owned unit, and "partial reads" force
  unit-native transports to fake a stream.
- **D2 (L2)**: establishment assumes byte offsets - the greeting is "the
  first 64 bytes" and commands are stream-delimited.
- **D3 (L3)**: the encoder/parser re-derive delimiters the transport already
  has; on a delimiter-native transport the length prefixes are pure overhead
  and the materialization dance exists only to bridge caller buffers against
  wire buffers.
- **D4 (L0)**: `OnAccept` delivers only the byte connection, so a
  unit-native transport must impersonate a stream.

BCL anchors: `WebSocket`/`ValueWebSocketReceiveResult` (a message-unit read
is a *result discriminant*, never a byte count, and a unit API never inherits
`Stream`), `ChannelReader<T>` (unit reads are ownership transfers),
`ConnectionContext`/`IConnectionListener` (one listener contract per
currency; protocols plug above a unified connection), and
`WebSocket.CreateFromStream` (framing is an adapter over a byte transport).
0030's rejection of BCL Pipelines stands: the byte contract keeps
caller-supplied buffers.

## 2. The changed model

Three layers, each a plug point:

```
L3  protocol (Message semantics)   IZWireProtocol: establishment (greeting +
                                   mechanism) + session (send gate, pump).
                                   Built-in: ZmqProtocol (ZMTP 3.0).
                                   Plug: ZSocketOptions.WireProtocol.
L2  frame layer                    Currency: ZWireFrame (ZmtpFrameFlags + the
                                   library's own ownership-carrying ZFrame).
                                   Protocol-related by design: the flags and
                                   the delimiters belong to the dialect.
                                   Plug A: IZFraming - bytes→frames adapter
                                   (ZmtpFraming is the built-in dialect).
                                   Plug B: IZFrameTransport - a native frame
                                   source (in-proc channels, a future
                                   WebSocket transport).
L1  byte layer                     IZConnection - unchanged, shipped; the
                                   Stream analogue. Plug: IZTransport.
```

Compositions: tcp/ipc = L1 + ZmtpFraming + ZmqProtocol (the decomposed
original stack); an in-proc transport = L2-native + ZmqProtocol; a future
WebSocket transport = L1 + a WS framing (or L2-native after upgrade) + a ZWS
protocol variant. `IZFrameConnection` is a *sibling* of `IZConnection`, not
a subtype - no unimplementable inherited members (the LSP lesson from the
rejected first draft).

The runtime composes: a byte peer adapts its framing (bounded by the command
limit during establishment), runs the protocol's establisher, then **rebinds
the framing reader in place** - the codec is known only after establishment -
to the zero-copy allocator path (NULL codec) or the codec-aware encoded
bound. Everything above the pump (socket-type checks, READY metadata,
queues, materializers, lifecycle) is shared and unchanged.

## 3. Existing API impact (0.9.0 breaking changes)

- **Removed** (absorbed by the decomposition, per the repo's retirement
  precedent - 0022 factory retirement, 0030 surface cuts):
  `ZmtpParser` (public parser), `ZmtpFrameEncoder` (public encoder). Their
  substance lives on as `ZmtpFraming` (reader/writer dialect) and
  `ZmqProtocol`'s pump (command consumption, decode, delivery).
- **Added**: `ZWireFrame`, `IZFrameConnection`, `IZFrameTransport<>`,
  `IZFraming`/`ZmtpFraming`, `IZWireProtocol`/`IZWireEstablisher`/
  `IZWireSession`/`IZWirePump`/`ZmqProtocol`, `ZFrameAllocator` (publicized),
  `ZSocketOptions.WireProtocol`.
- **Adjusted**: `BindAsync/ConnectAsync<TEndpoint,TTransport>` gain an
  optional `IZFraming?` overload family (the merge that avoids a parallel
  `BindFramedAsync` family); `BindFrameAsync`/`ConnectFrameAsync`/
  `UnbindFrameAsync`/`DisconnectFrameAsync` serve native frame transports
  (constraint-only overloads cannot merge).
- **Unchanged**: `IZByteReader`/`IZByteWriter`/`IZConnection`/`IZTransport`/
  `SocketTransport`, the message model (`ZFrame`/`ZMessage`/`ZSegment`), the
  mechanism four (`IZSecurityMechanism` etc. - `ZMechanismContext`'s public
  byte constructor adapts through the frame layer internally), `IZFrameCodec`
  (a frame-body transform, orthogonal to the unit encoding), the string
  endpoint facade.

Mechanism sessions are byte-identical: `ZMechanismContext` is parameterized
over an internal `IZCommandChannel` (one command = one command frame) and
NULL/PLAIN/CURVE run unmodified over any transport through the frame layer.

## 4. Performance invariants preserved

The decomposition preserves the old stack's hot-path properties, enforced by
the unchanged allocation gates: sends are one logical write per frame
(FrameSequence gathering); the receive path materializes frames directly
into the receive policy's segments (the allocator now rides the framing
reader, rebound after establishment); the pooled (no-policy) tier stages
into one per-connection scratch and hands out borrowed views (the old
parser's discipline) - the reused staging removed a per-frame pool-rent
owner allocation that the first cut introduced. CURVE sends stage codec
output in a reused session buffer under the send gate.

## 5. Tests

Behavior preservation: the full pre-existing suite (interop included) runs
green against the decomposed stack - the 832-test net was the refactor's
safety line, and three accounting assertions encode architecture facts
rather than behavior (scratch lifetimes, in-flight delivery convergence).
New integration proofs (`FrameLayerIntegrationTests`): a native in-proc
frame transport end to end through `BindFrameAsync`/`ConnectFrameAsync`; a
custom 4-byte length-prefix framing over the stock socket transport through
the `IZFraming` overload; a custom greeting-less wire protocol through
`ZSocketOptions.WireProtocol` - each plug point exercised by a real socket
pair, not a mock.
