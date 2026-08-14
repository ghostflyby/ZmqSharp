# 0014 - XPUB/XSUB Pattern Cores

Status: draft
Date: 2026-08-11

Designs the XPUB and XSUB pattern cores on the 0007 architecture, following
libzmq semantics: XSUB sends subscription frames manually and applies no
inbound filter; XPUB broadcasts messages and exposes the subscription frames
it receives to the application for observation, forwarding them to peers.

## 1. Wire semantics

- XSUB: `Subscribe`/`Unsubscribe` send the libzmq wire frames `0x01`+topic /
  `0x00`+topic to every connected peer; inbound messages are delivered
  unfiltered (the publisher side decides what to send).
- XPUB: outbound is the PUB broadcast (topic = first frame). Inbound frames
  are delivered to the bound sink (so the application observes
  subscriptions), and a subscription frame is also forwarded to every peer
  except the one it came from (upstream propagation, libzmq default verbose
  behavior).

## 2. XSUB pattern core

Same subscription propagation as SUB (0013) but the topic filter is removed:
every inbound message reaches the sink. `RouteOutbound` is rejected.

## 3. XPUB pattern core

Same broadcast as PUB (0013) plus subscription observation:

- Inbound (via the semantic-seam hook) is delivered to the sink unchanged.
- A received subscription frame is re-broadcast to every peer except the
  sender, so an upstream publisher learns the subscription.

## 4. Public shapes

```csharp
new ZXPubSocket(ZSocketOptions?)  -> ZXPubSocket   // broadcast + subscription observation
new ZXSubSocket(ZSocketOptions?)  -> ZXSubSocket   // manual subscription frames, no filter
```

Socket-Type compatibility: XPUB <-> XSUB (and PUB/SUB interop where libzmq
permits).

## 5. Interop acceptance

Both directions over TCP: NetMQ XSub <-> ZmqSharp XPub (subscription frame
observed and data broadcast back), ZmqSharp XSub <-> NetMQ XPub
(subscription frame sent, unfiltered data received).

## 6. Non-goals

- STREAM (requires RFC 23/37 routing-id wire metadata and stream-notify
  semantics; deferred to a wire-prerequisite slice).
- XPUB subscription-state management beyond observation (libzmq
  `ZMQ_XPUB_MANUAL` mode).
