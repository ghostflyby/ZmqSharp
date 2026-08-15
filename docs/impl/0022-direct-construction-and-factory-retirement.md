# 0022 - Direct Construction and Factory Retirement

Status: accepted
Date: 2026-08-14
Revision: 1

Retires the `ZSocket` static factory. Every socket is constructed directly
with `new`; set-once configuration lives in `ZSocketOptions` as `init`
properties. The only repeatable surface is endpoint management: `BindAsync` /
`ConnectAsync` / `UnbindAsync` / `DisconnectAsync`, callable any number of
times in any order (0002, 0020, 0021).

**Partially superseded by 0023**: the queue surface is no longer an explicit
two-object composition (`new ZQueueSocket<T>(new T(...), options)`) - the
wrapper is retired and the queue surface is the default surface of the socket
itself (`ZQueueSocketBase`). The factory-retirement rule (set-once is `init`
/ constructor; endpoints are the repeatable surface) stands unchanged.

## 1. Problem

The factory existed to provide "one-line creation", but it grew a second
configuration bag to do it. `ZQueueSocketOptions` carried `Pool` and `Security`
whose only consumer was the factory's private `ToSocketOptions` forwarding
onto the wrapped socket's `ZSocketOptions` - duplicated configuration on the
other bag, invisible to the caller's initializer. The factory also forced the
`*Callback` naming split (`CreatePair` / `CreatePairCallback`) to distinguish
two surfaces that the constructor signature already distinguishes.

The deeper issue was the set-once surface. C# gives `init` properties exactly
two legal assignment slots - constructor and object initializer - and a static
factory returns a fully built object, so no factory form can take an object
initializer. Every set-once knob either had to become a factory parameter (a
breaking signature change per knob) or a post-construction `Set*` method (a
two-phase init the project explicitly wanted to avoid). Direct `new`
construction removes the entire tension: set-once is expressed by `init`
properties and constructors, enforced by the compiler.

## 2. The rule

- **Set-once, exactly-once** configuration (socket type, identity, pool,
  security mechanism, handshake limits, queue factories, receive policy) is a
  constructor parameter or an `init` property. An `init` property can only be
  assigned during construction, so "configured at most once, before use" is
  enforced by the language.
- **Repeatable, append-only** surface (bind/connect endpoints) is a method,
  callable any number of times. `BindAsync` / `ConnectAsync` add one endpoint
  per call and may be interleaved (libzmq: all types except PAIR accept
  multiple binds and connects; PAIR restricts the peer *count* semantically,
  through `ZSinglePeerDispatch`, not through the API shape).

This leaves no `Set*` methods on the public socket surface. The wiring seams
that remain (`BindMessageSink`, and the internal `SetPeerConnectedHandler` /
`SetReceiveMaterialization`) are not user set-once knobs: the queue wrapper
binds its own sink and materialization to an already-constructed callback
socket inside its constructor, and a wrapping type cannot assign a `new`-built
socket's `init` property, so these stay methods.

## 3. The new surface

```csharp
// Callback surface: construct the composition root directly.
var req = new ZReqSocket();                                          // defaults
var rep = new ZRepSocket(new ZSocketOptions { Security = curve });   // configured

// Queue surface: two explicit objects - the core and its channel wrapper.
var pull = new ZQueueSocket<ZPullSocket>(
    new ZPullSocket(new ZSocketOptions { Pool = pool }),
    new ZQueueSocketOptions { ReceiveQueueFactory = new BoundedChannelOptions(16) });

// Endpoints remain the repeatable surface.
await pull.BindAsync("tcp://*:5555");
await pull.ConnectAsync("tcp://peer:5555");
```

Every concrete socket's public constructor takes `ZSocketOptions? options =
null`, defaulting to a fresh options bag. `ZQueueSocket<TSocket>`'s
constructor became public (`ZQueueSocket(TSocket socket, ZQueueSocketOptions?
options = null)`).

## 4. Removals

- `ZSocket` (the factory class) and every `*Callback` naming.
- `ZQueueSocketOptions.Pool` and `ZQueueSocketOptions.Security`: the wrapper
  never reads `Security` and never rents from `Pool` (its send path delegates
  to the inner socket); both belonged to the socket, whose options bag already
  carried them.

## 5. Migration

`ZSocket.CreateX(...)` maps mechanically: direct sockets to `new ZXSocket(...)`,
queue-surface sockets to `new ZQueueSocket<T>(new T(...), options)`, and
`Pool`/`Security` on a queue options bag to the inner socket's `ZSocketOptions`.
All 302 tests were migrated without behavioral change.

## 6. Consequences

- One configuration surface per object; the two-bag confusion is gone.
- New set-once knobs land as `init` properties - additive, non-breaking.
- The queue surface pays one extra `new` for explicit two-tier composition,
  and users of the callback surface drop the `*Callback` suffix.
- Design documents 0002, 0007, 0010, 0011, 0013, 0014, 0016, 0018 and the
  README were updated to the direct-construction shapes.
