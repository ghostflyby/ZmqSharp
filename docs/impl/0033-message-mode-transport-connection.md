# 0033 - Message-Mode Transport Connections

Status: accepted
Date: 2026-11-14

0030 fixed the transport seam at byte I/O: `IZConnection` exposes partial
reads, complete writes and `Abort`, and nothing else. The ZWS WebSocket
implementation (draft PR #79) tested that seam against a transport class it
did not anticipate - one whose wire format is **message-delimited natively**
(WebSockets; equally in-proc channels or datagram-style transports) - and the
byte-only shape failed there in three concrete ways. This document records
the resulting additive abstraction, `IZMessageConnection`, and why it does
not reopen the 0030 decisions it is adjacent to.

## 1. What the WebSocket implementation proved

- **Boundary loss.** ZWS framing is "one ZMTP frame per binary WebSocket
  message": frame boundaries *are* message boundaries. A framing layer above
  a byte view cannot reconstruct them (there is no length prefix - that is
  the point of the mapping), so the WebSocket connection had to expose
  private message-preserving primitives next to the interface's byte members
  and smuggle them to the framing layer via marker-interface downcasts.
- **Empty-message/EOF ambiguity.** The byte contract defines a zero-byte read
  as EOF. A zero-length message is valid WebSocket traffic; through a byte
  view it is indistinguishable from a close. The implementation had to reject
  empty messages *at the transport* to protect the byte view - a framing
  concern forced into the wrong layer.
- **Private escape hatches multiply.** Once message primitives exist as
  implementation details, every framing consumer needs its own path to them;
  the seam stops being the contract.

## 2. The decision

`IZMessageConnection : IZConnection` (public, additive) with two members:

- `ReadMessageAsync(long maxBytes, CancellationToken)`: one whole message
  into an owned buffer; the returned memory is borrowed until the next read;
  null means EOF; **an empty memory is a valid zero-length message** - the
  distinction is the reason this interface exists. A message longer than
  `maxBytes` faults the connection: the transport bounds its receive buffer
  before any framing layer sees the bytes.
- `WriteMessageAsync(ReadOnlySequence<byte>, CancellationToken)`: one
  message per call, segments gathered; completion consumes the entire input;
  boundaries are preserved across consecutive calls.

Concurrency, abort and dispose semantics are inherited from `IZConnection`
unchanged. The inherited byte members are an *optional fidelity projection*:
reads may serve message payloads as a stream and writes may map one call to
one message, but where a faithful view is impossible (the empty-message/EOF
ambiguity above), the byte members may throw `NotSupportedException`.
Protocol framing must use the message members.

### What this does not reopen

- **Framing still belongs above the connection.** 0030's central lesson - the
  CURVE codec confusion - was that frame transformation is a session-layer
  concern (`IZFrameCodec`). `IZMessageConnection` admits transport-native
  boundaries; it does not move frame parsing or codecs into the transport.
- **No callbacks or sinks return.** The removed `IZMessageSink` /
  `IZWriteSink` shapes mixed transport, wire send and receive callbacks in
  one interface (0028 A1/A3). The new interface is pull-shaped I/O, like the
  byte seam.
- **Byte transports are unchanged.** ZMTP is a length-prefixed stream
  protocol; `ZmtpParser`, `ZmtpSession`, `SocketRuntime` and `PeerRecord`
  stay exactly as they are. The runtime continues to hold connections as
  `IZConnection`.

### Orthogonality

Message mode is independent of the (internal, WebSocket-PR)
`IZProtocolConnection` axis - "the connection carries its own wire
protocol": bare `ZWS2.0` is message-mode *without* being a protocol
connection (no greeting-less establishment to drive), and a custom
length-delimited protocol over TCP can be a protocol connection *without*
being message-mode. The two axes compose.

## 3. Tests

`ZmqSharp.Tests/Transports/MessageConnectionContractTests.cs` locks the
contract through a scripted fake (`ScriptedMessageConnection`) that reuses
one receive buffer, mirroring the byte-contract suites' discipline
(`ZSocketConnectionTests` for delivery shape, `RuntimeLifecycleTests` for
abort/lifetime): whole-message delivery without partials; the
empty-message/null-EOF distinction; borrowed-memory lifetime (valid until the
next read, snapshot-to-retain); segment gathering with one call = one
message; the `maxBytes` fault; abort unblocking a pending read, idempotent;
and the documented `NotSupportedException` byte-view opt-out.

## 4. Follow-ups

- The WebSocket transport (draft PR #79) rebases onto this:
  `WebSocketConnection` implements `IZMessageConnection`, its private
  `TryReceiveMessageAsync`/`SendFrameAsync` twins are deleted in favor of the
  interface members, and the empty-message rejection moves from the
  connection into `ZwsPump` where it belongs.
- The shipped surface ships ahead of an in-tree implementer by design:
  transports are the public extension point, and the WebSocket connection is
  the motivating consumer waiting in draft.
