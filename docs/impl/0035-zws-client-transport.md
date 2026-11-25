# 0035 - The ZWS Client Transport

Status: accepted
Date: 2026-11-17

Refines 0033's WebSocket composition and 0034's native-connection path. The
earlier WIP took the byte-framing route and hand-rolled the HTTP upgrade and
WebSocket server; this document records the client-only native-connection
model that replaced it.

## 1. Why the byte-framing route was abandoned

RFC 45 selects the security mechanism with the WebSocket subprotocol, e.g.
`ZWS2.0/NULL`. A `/` is not an RFC 2616 token character, so RFC 6455 clients
that validate the subprotocol grammar (`ClientWebSocket`, browsers) cannot
offer it at all; libzmq only interoperates because its `ws_engine` hand-rolls
the HTTP handshake.

A proposal in libzmq issue #4951 recommends changing the separator from `/`
to `+` (`ZWS2.0+NULL`, `ZWS2.0+PLAIN`, `ZWS2.0+CURVE`). No RFC revision has
been accepted: the published RFC 45 is still a draft that defines only the
slash names, and current libzmq servers match those names exactly. `+` is a
legal token, so the BCL client can perform the upgrade itself; that removes
every reason to hand-roll HTTP and WebSocket framing, and the server half -
the only part that would still need a hand-written upgrade - is dropped.

The client therefore offers the proposed `ZWS2.0+NULL` first and the bare
`ZWS2.0` no-mechanism profile second. A server that implements the proposal
selects the first; a current libzmq server configured with the NULL mechanism
selects the second, so the bare profile restores interop with existing
deployments. A server that accepts only the draft's slash spellings still
cannot complete the upgrade with the BCL client; that remains an open item
until RFC 45 is revised.

## 2. The model

ZWS is a native message transport under 0034, not a framing over bytes:

```
L3  ZwsProtocol (internal)     no greeting; '+' selects the mechanism and
                               NULL runs its READY exchange; the bare
                               ZWS2.0 profile exchanges the routing identity
L2  ZwsConnection (internal)   IZMessageConnection: one ZWS frame per WS
                               binary message, MORE chains aggregated
L1  WebSocketTransport         IZClientTransport<..., WebSocketEndpoint>:
                               ClientWebSocket + ZWS2.0+NULL / ZWS2.0
```

- `ZwsConnection` has no byte origin. It enforces the receive composition
  while reading: the frame bound, the accumulated message bound, and the
  remaining message budget cap the buffer growth, so an over-limit peer is
  rejected before its whole message is buffered. The runtime's post-delivery
  message validation remains the backstop for connections that ignore the
  composition.
- One ZWS frame is one binary WebSocket message: a flag byte followed by the
  body. The flag values are `0x00` final, `0x01` more, `0x02` command - not
  the ZMTP values (`0x02` there is long-size, `0x04` command), so the mapping
  is explicit in `ZwsWire`.
- Reads re-aggregate a MORE chain into one `ZMessage`; commands surface as
  command units and are consumed by the shared message session. Text
  messages, empty messages, reserved flag bits, and command+MORE are protocol
  violations. A size-limit rejection closes with `MessageTooBig` (1009),
  other protocol failures with `ProtocolError` (1002), and a peer Close is
  answered with `NormalClosure` (RFC 6455 5.5.1/7.1.7/7.4.1). A locally
  enforced limit also raises the socket's receive-rejection counter and
  signal, matching the materializer path.
- The bare `ZWS2.0` no-mechanism profile (RFC 45) has no READY metadata: each
  peer's first message is its routing identity, followed by regular messages.
  Without a Socket-Type property the RFC's compatibility SHOULD cannot be
  evaluated, so the runtime accepts the peer and skips that check. The local
  identity comes from the same READY body the ZMTP mechanisms use; socket
  types that do not advertise one send an empty identity frame.
- Sends are one unfragmented binary message per frame (libzmq rejects
  fragmented ZWS messages). The connection copies during the call, so
  `RetainsMessages` is false.
- The established session is the same `ZMessageSession` ZMTP uses; the codec
  path is retained for a future `ZWS2.0+CURVE`, though this version offers
  NULL and the no-mechanism profile only.

## 3. Client-only seam

`IZClientTransport<TSelf, TEndpoint>` is the connect-only transport contract;
the symmetric listen-only seam is `IZServerTransport<TSelf, TEndpoint>`, and
a full-duplex transport implements both (0036). `WebSocketTransport`
implements only the client seam, so `BindAsync` on the typed surface does not
exist at compile time. The string facade still accepts the scheme and reports
`NotSupportedException` when asked to bind.

The runtime selects the protocol per connection: a connection implementing
the internal `IZProtocolCarrier` (the ZWS connection) supplies
`ZwsProtocol`, while an explicit `ZSocketOptions.WireProtocol` keeps
precedence.

## 4. Consequences

- The proposed `+` name is not part of RFC 45 yet; current libzmq servers only
  match `/`. The bare `ZWS2.0` fallback is the RFC-mandated profile and is
  what those servers select when configured with NULL. A server that accepts
  only the slash names remains unreachable through the BCL client, so the
  interop limitation is documented rather than hidden.
- `ws://`/`wss://` endpoints follow the BCL `Uri` form: default ports are
  omitted from the canonical string, IPv6 literals are bracketed, a missing
  leading slash is added, and a fragment is ignored (it is not sent on a
  WebSocket request). Invalid host/path/port combinations are rejected at
  construction instead of surfacing as `UriFormatException` during connect.
- wss uses `ClientWebSocket`'s TLS stack, including the system certificate
  policy. There is no hook for a custom validation callback yet.
- WebSocket keepalive probes are disabled (`KeepAliveInterval` zero): the ZWS
  traffic is the liveness signal.
- The test suite supplies a test-only server harness (HTTP upgrade on a
  `TcpListener` plus `WebSocket.CreateFromStream` in server mode); the library
  itself ships no server half.
- The negotiated mechanism must match the socket configuration. A socket
  configured for PLAIN (or any non-NULL mechanism) over a ZWS endpoint fails
  establishment with `NotSupportedException` instead of silently running
  NULL; the bare `ZWS2.0` profile is only accepted for a NULL-configured
  socket.
