# 0024 - Socket-Owned Send Surfaces

Status: accepted
Date: 2026-08-14
Revision: 1

Removes the generic `SendAsync` from `IZSocket` and demotes the base send
mechanism to `protected SendAsyncCore`. Each socket type now exposes its own
public send surface. This realizes a design vision the docs have stated since
0006/0007: "It must not preserve `IZSocket.SendAsync` or public channel pairs
solely for compatibility with the current prototype" (0006 section 6) and
"There is no general `IZSocket.SendAsync` surface contract" (0007 section 2).

## 1. Problem

`IZSocket` declared a generic `SendAsync(ZMessage)` / `SendAsync(bytes)`, and
every socket inherited its implementation from `ZSocketBase`. On most types
that surface is the real send path - but on ROUTER, REP, SUB, and PULL it
**always throws** (their dispatch policies have no generic-send answer), and
on REQ it is only legal while a request is in flight. The interface promised
an operation that a third of the types cannot perform; the exception message
pointing at the correct API was runtime first aid for an API that should not
exist. This was the same abstraction-leak family as `IZCallbackSocket`
(retired in Batch 1 of the redundancy sweep) and the silently-ignored queue
options (Batch 2): the type system promised something the instance does not
honestly support.

## 2. The split

- `IZSocket` is now endpoints only: `BindAsync` / `ConnectAsync` /
  `UnbindAsync` / `DisconnectAsync` (+ `IAsyncDisposable`). There is no
  send contract, and no receive contract (0023).
- `ZSocketBase` keeps the generic send *mechanism* as protected
  `SendAsyncCore(ZMessage)` / `SendAsyncCore(bytes)`, plus the private
  `SendToTargetsAsync`/`SendToPeerAsync` and the internal `SendToAsync`
  (directed send). The routing machinery is intact; only its exposure moved.
- Each concrete type decides its own public send surface:

  | Type | Public send surface |
  |---|---|
  | PAIR, DEALER, PUSH, PUB (and XPUB via PUB) | `SendAsync` x2, forwarding `SendAsyncCore` |
  | ROUTER | `SendAsync(byte[] identity, ...)` x2 (own overloads) |
  | REQ | `RequestAsync` only; the core sends frames through an internal `SendRequestFrameAsync` |
  | REP | `SendReplyAsync(context, reply)` only |
  | PULL, SUB, XSUB | no send member at all |

The two tests that pinned the throwing generic send (`Pull_SendThrows`,
`Req_GenericSend_WithoutInFlightRequest_Throws`) are deleted: the guarantee is
now compile-time - those types simply have no `SendAsync`. The policy-level
throwing contracts (the dispatch policies' `SelectTargets` exceptions) stay,
as do their unit tests.

## 3. Consequences

- A socket type's public members are exactly what it can do; there is no
  inherited surface that throws on principle.
- The fail-loudly messages survive where they still matter (the dispatch
  policies and the directed-send paths).
- Custom `ZSocketBase` subclasses now expose only what they declare; a custom
  type that wants to send adds its own `SendAsync` forwarding
  (`SendAsyncCore`), and one that does not simply leaves it out.
- Known limitation, unchanged from before: `SendQueueFactory` on a
  receive-only socket (PULL/SUB/XSUB + outbound channel) still fails at
  runtime through the dispatch policy rather than at construction. The
  outbound channel is a generic queue-surface feature; closing that gap is
  out of scope for this document.

## 4. Migration

Sockets typed through `IZSocket` lose `SendAsync`: the interface is endpoints
only, so interface-typed callers (the string-endpoint extension facade) were
already send-free. Concrete send-capable types (PAIR/DEALER/PUSH/PUB) keep
the same `SendAsync` signatures - all 68 socket-typed test call sites compile
unchanged. Two custom test subclasses added forwarding; two
throwing-send tests were deleted. 311 tests pass.
