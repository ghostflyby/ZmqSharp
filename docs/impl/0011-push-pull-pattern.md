# 0011 - PUSH/PULL Pattern Cores

Status: draft
Date: 2026-08-11

Designs the PUSH and PULL pattern cores on the 0007 architecture, following
libzmq semantics: PUSH is send-only with round-robin outbound; PULL is
receive-only with fair-queue inbound. Both are implemented as IPatternCore
compositions with no new surfaces (PUSH uses the generic SendAsync; PULL uses
the channel surface).

## 1. Wire semantics

Neither pattern frames messages: PUSH sends the message as-is, PULL receives
it as-is. Multipart is preserved (0001 D3).

## 2. PUSH pattern core

- Send-only: `RouteOutbound` round-robins across all connected peers with an
  unconditionally advancing cursor (no starvation, same pattern as DEALER).
  An empty peer set drops the message (the generic send path). No receive
  surface is exposed beyond the inherited raw callback (a compatible PULL
  peer never transmits).

## 3. PULL pattern core

- Receive-only: `RouteOutbound` throws `InvalidOperationException` so the
  generic send path is rejected (the queue surface's `SendAsync` forwards to
  it).
- Fair-queue inbound: the transport core's per-peer pumps feed the channel
  surface's aggregate reader, whose fixed-order scan of the peer snapshot
  approximates fair intake with an ordering bias (a busy early-connected peer
  can dominate; a rotating libzmq-style fair-queue is a non-goal, section 6).

## 4. Public shapes

```csharp
public sealed class ZPushSocket(ZSocketOptions? options = null) : ZQueueSocketBase(...)   // SendAsync only
public sealed class ZPullSocket(ZSocketOptions? options = null) : ZQueueSocketBase(...)    // queue surface by default (0023)

new ZPushSocket()   -> ZPushSocket
new ZPullSocket()   -> ZPullSocket   // Messages reads the per-peer queues
```

Socket-Type compatibility: PUSH <-> PULL.

## 5. Interop acceptance

Both directions against NetMQ over TCP (0006 section 5): ZmqSharp PUSH ->
NetMQ PULL, NetMQ PUSH -> ZmqSharp PULL, round-robin distribution across two
PULL peers, and PULL send rejection.

## 6. Non-goals

- Per-peer send queues (0004 D2): PUSH routes per message, no queuing.
- Fair-queue fairness policies beyond the existing aggregate reader.
