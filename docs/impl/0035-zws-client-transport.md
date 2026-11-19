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

The pending RFC 45 revision changes the separator from `/` to `+`
(`ZWS2.0+NULL`, `ZWS2.0+PLAIN`, `ZWS2.0+CURVE`). `+` is a legal token, so the
BCL client can perform the upgrade itself. That removes every reason to
hand-roll HTTP or WebSocket framing, and the server half - the only part that
would still need a hand-written upgrade - is dropped.

## 2. The model

ZWS is a native message transport under 0034, not a framing over bytes:

```
L3  ZwsProtocol (internal)     no greeting; the subprotocol selected the
                               mechanism; NULL runs its READY exchange
L2  ZwsConnection (internal)   IZMessageConnection: one ZWS frame per WS
                               binary message, MORE chains aggregated
L1  WebSocketTransport         IZClientTransport<..., WebSocketEndpoint>:
                               ClientWebSocket + ZWS2.0+NULL
```

- `ZwsConnection` has no byte origin. The receive composition is a no-op: the
  connection already owns whole frames and the runtime's message-level limits
  still apply after delivery.
- One ZWS frame is one binary WebSocket message: a flag byte followed by the
  body. The flag values are `0x00` final, `0x01` more, `0x02` command - not
  the ZMTP values (`0x02` there is long-size, `0x04` command), so the mapping
  is explicit in `ZwsWire`.
- Reads re-aggregate a MORE chain into one `ZMessage`; commands surface as
  command units and are consumed by the shared message session. Text
  messages, empty messages, reserved flag bits, and command+MORE are protocol
  violations.
- Sends are one unfragmented binary message per frame (libzmq rejects
  fragmented ZWS messages). The connection copies during the call, so
  `RetainsMessages` is false.
- The established session is the same `ZMessageSession` ZMTP uses; the codec
  path is retained for a future `ZWS2.0+CURVE`, though this version offers
  NULL only.

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

- RFC 45 slash subprotocols are legacy server-side spellings; this client
  offers `ZWS2.0+NULL` only, per the revision's "native clients offer the `+`
  name" rule.
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
  NULL.
