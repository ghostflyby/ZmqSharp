# 0025 - Routing Identity and ROUTER Peer Routing

Status: accepted
Date: 2026-08-16
Revision: 1

Adds a configurable ZMTP routing identity (the `Identity` READY metadata
property) and makes ROUTER route on the identity peers advertise, instead of
the local counter-assigned ids it uses today. This is the missing piece for a
Jupyter migration: a Jupyter client opens shell and stdin DEALER sockets with
the **same** routing identity, and the kernel's ROUTER sockets address peers
by that identity; without it the client cannot talk to a libzmq/Deno/Python
kernel, and multiple frontends on one ZmqSharp kernel mis-route stdin.

## 1. Problem

Four concrete gaps, each verified in the current code:

- **No configurable routing identity.** `ZSocketOptions` has no identity
  option (ZSocketOptions.cs). A socket cannot advertise an `Identity` in its
  READY.
- **READY carries only Socket-Type.** `ZmtpCommands.BuildReady(socketType)`
  writes exactly one metadata property (ZmtpCommands.cs:20-21). There is no
  `Identity` property on the wire.
- **Peer identities are ignored.** `ZmtpCommandCodec.ParseReadySocketType`
  reads only Socket-Type from the peer READY; the `Identity` property, if a
  peer sends one, is parsed into the property dictionary and thrown away.
- **ROUTER assigns its own ids.** `ZIdentityDispatch.AssignIdentity` gives
  each peer a locally-incremented 4-byte id, never the peer's advertised
  identity (ZDispatchPolicies.cs:50-64). The inbound policy prefixes that
  local id; `SendAsync(identity, ...)` resolves through the same table.

The Jupyter failure this causes is precise. The Maieutics Jupyter client opens
shell and stdin DEALER sockets with one shared identity (a fresh Guid) and the
control DEALER with another
(`NetMqJupyterTransport.cs`: `shell`/`stdin` share `serialization.ClientIdentity`,
`control` uses `Guid.NewGuid()`). The kernel routes shell replies, stdin
requests, and control replies back to the **same client socket instance** by
that identity. With ZmqSharp:

- A ZmqSharp client cannot set the identity at all, so a real Jupyter kernel
  (libzmq / Deno / Python) cannot route stdin back to the requesting client -
  stdin dies, which is a protocol-correctness break, not a cosmetic one.
- A ZmqSharp ROUTER kernel assigns each peer an independent counter id **per
  ROUTER socket**. Shell, control, and stdin are three separate ROUTER
  sockets, so the same client gets three unrelated ids; which id maps to which
  peer depends on connection arrival order. Two frontends connected to one
  kernel can cross-wire: shell messages from frontend A routed to frontend B's
  stdin.

## 2. Wire format: the ZMTP 3.1 Identity metadata property

ZMTP 3.1 (RFC 23) moves the routing identity from the ZMTP 3.0 identity frame
into a READY metadata property. Both libzmq and NetMQ send a DEALER/ROUTER
socket's configured identity as an `Identity` property in READY, verbatim
bytes; a peer that sends none is assigned a local id by the router.

- Property name: `Identity` (a metadata-name character set member).
- Value: 1-255 bytes, **opaque** - identity bytes are arbitrary, never decoded
  as text (a Jupyter Guid is not valid UTF-8, and `Guid.NewGuid().ToByteArray()`
  may legitimately start with `0x00`; the old ZMTP 3.0 rule that a leading
  `0x00` means "no identity" does **not** apply to the 3.1 metadata property).
- Empty or absent `Identity`: the peer does not advertise one; the router
  falls back to its local assignment.

## 3. Design

### 3.1 Outbound: `ZSocketOptions.Identity`

Add one option to `ZSocketOptions`:

```csharp
/// <summary>
/// ZMTP 3.1 routing identity advertised in this socket's READY (RFC 23
/// Identity metadata property). Opaque bytes, 1-255; empty (default) sends
/// no identity and the peer assigns a local one. The Identity property is
/// attached to the READY only for REQ, DEALER, and ROUTER sockets, matching
/// libzmq's add_basic_properties gate (mechanism.cpp): those are the roles
/// that can be addressed by a router. Other types accept the option but
/// never send it (libzmq stores it locally and omits it from the wire).
/// </summary>
public ReadOnlyMemory<byte> Identity { get; init; } = ReadOnlyMemory<byte>.Empty;
```

- Length > 255 rejected at construction, in the `init` accessor, following the
  `MaxCommandSize` validation pattern (ArgumentOutOfRangeException).
- The identity is copied once into the socket's `localReadyBody` at
  construction (`ZSocketBase.cs:108`), which is exactly the existing
  once-per-socket READY build - no per-connection cost, no retained reference
  to the caller's buffer. The option is never read after construction, so
  ownership of the backing array stays with the caller and the socket does not
  pin it. The REQ/DEALER/ROUTER gate is the socket type's
  `AdvertisesIdentity` flag (see below), so a PAIR configured with an
  identity builds a READY byte-identical to today - matching libzmq, which
  stores the option but omits the property.

`ZmtpCommands.BuildReady` gains an identity parameter and appends the property
when the type advertises one and the identity is non-empty:

```csharp
public static byte[] BuildReady(string socketType, ReadOnlyMemory<byte> identity = default)
```

The body uses the existing `ZmtpCommandCodec.WriteMetadataProperty` helper, so
the property is encoded by the same rules the parser enforces. When
`identity.IsEmpty` or the socket type does not advertise identities, the
built body is byte-for-byte what it is today.

`ZSocketType` gains the gate flag; the built-in types set it per the libzmq
matrix (only the router-addressable roles):

```csharp
/// <summary>
/// Whether this socket type attaches an Identity metadata property to its
/// READY. True for REQ, DEALER, and ROUTER only - the roles libzmq's
/// add_basic_properties gate includes (mechanism.cpp); every other type
/// accepts an Identity option but omits the property from the wire.
/// </summary>
public bool AdvertisesIdentity { get; init; } = false;
```

`ZSocketTypes.Req` / `Dealer` / `Router` set `AdvertisesIdentity = true`;
the remaining eight types keep the default, so their READY is unchanged even
when an identity is configured. The gate is evaluated once at construction
next to `ZSocketBase.cs:108`'s READY build.

### 3.2 Inbound: capture the peer's advertised identity

The peer READY metadata already reaches the socket layer:
`ZMechanismResult.PeerReadyBody` is the owned copy of the peer's READY
arguments, and `ZSocketBase.RunConnectionAsync` parses Socket-Type from it.
Add a codec helper that reads the `Identity` property **as raw bytes**:

```csharp
public static ReadOnlyMemory<byte>? ParseReadyIdentity(ReadOnlySpan<byte> metadata)
```

The existing `ParseMetadata` decodes values as UTF-8 strings, which is wrong
for opaque identity bytes; the new helper walks the same property sequence but
returns the value bytes untouched (or null when `Identity` is absent or
empty). `ParseReadySocketType` keeps its string path - Socket-Type is ASCII by
definition.

The socket layer then hands the advertised identity to the routing dispatch
through a new protected virtual seam on `ZSocketBase`, defaulting to no-op so
non-ROUTER sockets are untouched:

```csharp
protected virtual void OnPeerEstablished(IZConnection peer, ReadOnlyMemory<byte> advertisedIdentity) { }
```

Called in `RunConnectionAsync` right after the socket-type check succeeds and
before `established.TrySetResult()` (ZSocketBase.cs:592-615), so the identity
is registered before any message can be routed. The advertised identity is
copied at registration (it is a view over the owned `PeerReadyBody`, so a
copy is the registration's own storage).

### 3.3 ROUTER routes on the advertised identity

`ZIdentityDispatch` keeps its content-addressed tables (identity bytes keyed
by latin1 string, peer keyed by connection) and gains one registration
method:

```csharp
/// <summary>Registers the peer's advertised routing identity (from READY); rejects a duplicate.</summary>
internal bool TryRegisterIdentity(IZConnection peer, ReadOnlyMemory<byte> advertisedIdentity)
```

- On registration with a non-empty identity: record the mapping **peer ->
  identity** and **identity -> peer**. If the identity already maps to a
  different live peer, return false and the new connection is refused at
  establishment (the libzmq ROUTER behavior: a second peer claiming an
  in-use identity is rejected, never silently shadowed).
- `AssignIdentity` (ZDispatchPolicies.cs:50) becomes: return the registered
  advertised identity when present, otherwise assign the local counter id as
  today. The inbound policy therefore prefixes the peer's **advertised**
  identity when it has one, and the local id only for peers that sent none -
  exactly the libzmq routing model.
- `TryResolve` is unchanged: `SendAsync(identity, ...)` addresses the peer by
  content, which is now the peer's advertised identity.
- `RemovePeer` (teardown) already cleans both maps; a refused registration
  never entered them.

### 3.3.1 libzmq reference (verified in libzmq master source)

libzmq's routing identity is **static configuration only** - there is no
strategy-function or callback hook anywhere in the option system:

- `zmq_setsockopt(socket, ZMQ_ROUTING_ID, blob, len)` copies a 1-255 byte
  opaque blob into `options.routing_id` (src/options.cpp); a socket has
  exactly one identity, and the READY handshake writes it verbatim as the
  `Identity` metadata property (src/mechanism.cpp: `ZMTP_PROPERTY_IDENTITY`).
- The router's `identify_peer` (src/router.cpp) uses the peer's advertised
  identity when the peer sends one (READY's Identity, surfaced to the pipe as
  a routing-id message); peers that send none fall back to an auto-generated
  5-byte `[0x00, uint32 counter]` with a random start
  (`_next_integral_routing_id` from `generate_random()`), incremented **per
  router socket**. That fallback is order-dependent and router-local by
  design - the connection-order problem is inherent to ZMTP peers that do not
  advertise an identity, in libzmq exactly as in ZmqSharp; the fix is that
  clients advertise one.
- Duplicate identities: `identify_peer` rejects the second peer (returns
  false, peer stays anonymous) unless `ZMQ_ROUTER_HANDOVER` is set, in which
  case the new connection takes the id over and the old pipe is re-identified
  and terminated. This design's reject-on-duplicate matches the libzmq
  default; handover is a future extension. (libzmq's random-start auto
  generation is also noted: ZmqSharp's counter starts at 0 today, both are
  local-only and never on the wire, so nothing to align except determinism.)

`ZRouterSocket` wires the seam by overriding `OnPeerEstablished`:

```csharp
protected override void OnPeerEstablished(IZConnection peer, ReadOnlyMemory<byte> advertisedIdentity)
{
    if (advertisedIdentity.IsEmpty) return;
    if (!dispatch.TryRegisterIdentity(peer, advertisedIdentity))
        throw new ZeroMqProtocolException("peer advertises an in-use routing identity");
}
```

Refusing the connection at establishment keeps the peer out of the routable
snapshot entirely: no partial routing table, no messages delivered, teardown
is the normal establishment-failure path.

### 3.4 Jupyter migration mapping

- **Client** (Maieutics.Jupyter.Client): shell and stdin become
  `new ZDealerSocket(new ZSocketOptions { Identity = clientIdentity })`; the
  shared identity is `serialization.ClientIdentity`, control keeps its own
  Guid - the identical socket arrangement NetMQ uses today, now expressible.
- **Kernel** (Maieutics.Jupyter.Kernel): shell/control/stdin stay
  `ZRouterSocket`; each now routes by the client's advertised identity, so all
  three sockets resolve the **same** client to the same identity bytes and
  stdin finds the requesting frontend regardless of connection order. Two
  frontends with distinct Guids never collide; a misconfigured duplicate
  identity is refused loudly instead of cross-wiring messages.

## 4. Compatibility

- **Default behavior unchanged.** No `Identity` configured: READY body is
  byte-identical, ROUTER assigns local ids as today, `ParseReadyIdentity`
  returns null and `OnPeerEstablished` no-ops. Existing ZmqSharp-to-ZmqSharp
  traffic is unaffected.
- **libzmq / NetMQ interop in both directions.** A NetMQ DEALER with
  `Options.Identity` sends the READY `Identity` property; a ZmqSharp ROUTER
  now parses it and routes on it. A ZmqSharp DEALER with `Identity` set
  advertises it; a libzmq/Deno/Python ROUTER routes stdin back to it - the
  exact interop the Jupyter client needs.
- **AOT / allocation constraints hold.** Identity is copied once at socket
  construction (README build) and once per peer at registration (cold
  establishment path, same tier as the existing identity assignment). No
  reflection, no dynamic code generation; validation is constructor-time.

## 5. Tests

- Codec: `ParseReadyIdentity` returns raw bytes for a Guid-shaped identity
  (including a leading `0x00` byte), null for absent/empty, and throws on the
  same malformed-property cases as `ParseMetadata`.
- READY build: `BuildReady(type, identity)` round-trips through
  `ParseReadyIdentity`; default build is byte-identical to today.
- Outbound: a DEALER with `Identity` connects to a NetMQ ROUTER and the router
  sees the advertised identity (interop proof).
- ROUTER inbound: a peer with `Identity` gets its advertised identity prefixed
  on delivery and `SendAsync(identity, ...)` routes to it; a peer without one
  gets a local id (existing tests stay green).
- Conflict: a second peer advertising an in-use identity is refused at
  establishment.
- Jupyter-shaped integration: one client, shell+stdin sharing an identity
  against a ROUTER - stdin replies reach the same peer deterministically
  across three ROUTER sockets; two clients with distinct identities never
  cross.
