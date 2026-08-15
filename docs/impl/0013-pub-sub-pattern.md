# 0013 - PUB/SUB Pattern Cores

Status: draft
Date: 2026-08-11

Designs the PUB and SUB pattern cores on the 0007 architecture, following
libzmq semantics: PUB is send-only broadcast where the message's first frame
is the topic; SUB is receive-only with topic-prefix subscription filtering
and subscription propagation to publishers.

## 1. Wire semantics

- PUB sends the message as-is (topic = first frame); every connected peer
  receives it.
- SUB filters inbound by matching the first frame (the topic) against its
  subscribed prefixes; the empty prefix subscribes to everything.
- Subscription propagation uses libzmq's wire convention: a message whose
  first frame is `0x01` + topic subscribes, `0x00` + topic unsubscribes.
  A publisher that receives no subscription drops all messages, so a SUB
  must announce its subscriptions for a NetMQ/libzmq PUB to start sending.

## 2. PUB pattern core

- Send-only broadcast: `SendAsync` overrides the generic single-target path
  and broadcasts to every peer in the snapshot (`BroadcastAsync`, a transport
  primitive); the message is disposed once after the loop.
- The base `RouteOutbound` (single target) is rejected for PUB.
- Socket-Type compatibility: PUB <-> SUB.

## 3. SUB pattern core

- `Subscribe(byte[] topic)` / `Unsubscribe(byte[] topic)`: content-matched
  prefix subscriptions, guarded by `StateLock`.
- Inbound filtering is a semantic-seam hook (`PrepareInboundForSink`): a
  non-matching message is disposed and dropped; matching messages reach the
  bound sink or channel surface. The hook generalizes the ROUTER identity
  prefix to also allow filtering (null = dropped).
- Subscriptions are announced to each peer on connect and on change via the
  `0x01`/`0x00` wire frames.
- `RouteOutbound` is rejected (receive-only).
- Socket-Type compatibility: SUB <-> PUB.

## 4. Public shapes

```csharp
new ZPubSocket(ZSocketOptions?)    -> ZPubSocket   // SendAsync broadcast
new ZSubSocket(ZSocketOptions?)    -> ZSubSocket   // Subscribe/Unsubscribe + queue surface (Messages, 0023)
```

## 5. Interop acceptance

Both directions over TCP: ZmqSharp PUB -> NetMQ SUB (subscribed topic
delivered, non-matching topic not), NetMQ PUB -> ZmqSharp SUB (matching
delivered after subscription propagation, non-matching filtered).

## 6. Non-goals

- Subscription matching beyond byte-prefix (libzmq default is prefix).
- Per-topic delivery guarantees (PUB is lossy by design).
- XPUB/XSUB subscription observation (later).
