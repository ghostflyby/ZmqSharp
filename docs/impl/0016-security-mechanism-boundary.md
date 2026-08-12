# 0016 - Replaceable Security Mechanism Boundary

Status: draft
Date: 2026-08-12
Revision: 1

Designs the ZMTP security mechanism boundary promised by 0015 section 3 and
0006 section 4: mechanisms are pluggable, not hard-coded to NULL; a mechanism
is an `IZConnection -> IZConnection` transform that runs its own handshake
sequence; the parser never sees the mechanism. NULL is the first mechanism
extracted behind the boundary; PLAIN is designed as the second (0015 section
3.3 cost split). CURVE stays deferred.

## 1. Problem

The NULL handshake is hard-coded in three places, plus one socket-layer
coupling:

1. **Greeting production**: `ZmtpFrameEncoder.BuildNullGreeting` bakes the
   `"NULL"` mechanism into a fixed 64-byte constant, and `BuildHandshake`
   coalesces that greeting with the READY command.
2. **Greeting validation**: `ZmtpParser.ReadGreetingAsync` rejects any
   mechanism field that is not `"NULL"`.
3. **Handshake parsing**: `ZmtpParser.ReadHandshakeAsync` hard-codes "expect
   READY next" and is the only place that consumes the peer's READY metadata.
4. **Driver coupling**: `ZSocketBase.RunConnectionAsync` writes
   greeting+READY, establishes through the parser, then validates the peer
   socket type - handshake mechanics and socket-type policy are interleaved.

`IZConnection` already documents the intended shape ("no handshake is built
in... so security mechanisms can vary freely"), and 0006 section 4 fixes the
completion gate for the boundary. 0015 section 3.2 fixes the mechanism
concept: a mechanism is a per-connection handshake state machine, and the
parser only parses traffic on the session connection it returns.

## 2. Design decisions

**D1 - One mechanism per socket, matched by name, never discovered.**
The socket is configured with exactly one mechanism instance. The handshake
driver compares that mechanism's `Name` against the peer's greeting mechanism
field; a mismatch faults the connection. This deliberately corrects the
phrasing of 0015 section 3.2 ("instantiate the configured mechanism for that
field"): instantiating *from* a wire string would require reflection or a
registry, both prohibited by 0006 section 2.5 and the AOT stance. Matching a
configured instance by name is AOT-clean and matches libzmq, where a socket
has one mechanism.

**D2 - Combination freedom is orthogonal, not stacking.**
ZMTP allows exactly one mechanism per connection, so mechanisms do not stack
with each other (0015 section 3.1). The boundary therefore stays a single
authentication mechanism; encryption is a transport concern (a TLS transport
wrapping TCP), not a mechanism. The boundary still supports a *wrapping*
session connection so CURVE - the one ZMTP mechanism that combines
authentication with encryption - can later return an encrypting wrapper
without changing the seam.

**D3 - Role comes from the connection direction, not the configuration.**
`ConnectAsync` yields a Client session (greeting `as-server = 0`); an
accepted connection yields a Server session (`as-server = 1`). A socket may
bind and connect at the same time, so a mechanism carries both role
configurations and the role selects the branch at handshake time.

**D4 - The session owns the command sequence; the socket layer owns
socket-type declaration.**
The session drives everything between the greeting and the established state,
including both READY commands, because READY *ordering* is mechanism-specific
(NULL: immediate; PLAIN: after WELCOME) while READY *content* is a socket-type
concern. The driver injects the local READY body (built from the local
Socket-Type, 0015 section 2.4) and reads the peer Socket-Type from the
session result.

**D5 - No traffic can interleave during the handshake, so writes need not be
coalesced.**
Every message send blocks on the establishment gate
(`WaitUntilEstablishedAsync`), so only handshake writes happen before
establishment, and the per-connection write gate serializes those. The
greeting+READY single-buffer coalescing in `BuildHandshake` is therefore not
load-bearing and is dropped; greeting and commands are separate gated writes.

## 3. Public seam

All types live in `ZmqSharp.Zmtp` (mechanisms are ZMTP protocol concepts; see
section 13 for the namespace question).

```csharp
public enum ZMechanismRole { Client, Server }

/// <summary>Advertised mechanism; creates one per-connection handshake state machine.</summary>
public interface IZSecurityMechanism
{
    /// <summary>Mechanism name advertised in the greeting (e.g. "NULL", "PLAIN").</summary>
    string Name { get; }

    /// <summary>Creates the handshake state machine for one connection.</summary>
    IZMechanismSession CreateSession(ZMechanismRole role);
}

/// <summary>Per-connection handshake: runs the mechanism command sequence, then yields the session connection.</summary>
public interface IZMechanismSession
{
    ValueTask<ZMechanismResult> RunAsync(ZMechanismContext context, CancellationToken token = default);
}
```

`ZMechanismContext` is the connection-scoped wire view the session drives on.
It is public because users implement the seam; its framing machinery is
internal but reachable only through these members:

```csharp
public sealed class ZMechanismContext
{
    /// <summary>The raw connection; also the session connection for cleartext mechanisms.</summary>
    public IZConnection Connection { get; }

    /// <summary>Local READY body built by the socket layer; the session sends it at the protocol-correct point.</summary>
    public ReadOnlyMemory<byte> LocalReadyBody { get; }

    /// <summary>Command-frame size limit shared with the traffic parser (0008 Slice B).</summary>
    public int MaxCommandSize { get; }

    /// <summary>Writes one command frame (header + body) under the connection write gate.</summary>
    public ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);

    /// <summary>
    /// Reads one command frame. The returned name/arguments are borrowed from
    /// the context's scratch buffer and stay valid until the next read - the
    /// same lifetime rule as the parser's borrowed frames. Returns null on EOF.
    /// </summary>
    public ValueTask<ZMechanismCommand?> ReadCommandAsync(CancellationToken token = default);
}

```csharp
public readonly struct ZMechanismCommand
{
    public ZMechanismCommand(ReadOnlyMemory<byte> name, ReadOnlyMemory<byte> arguments);
    public ReadOnlyMemory<byte> Name { get; }       // e.g. "WELCOME", "HELLO", "READY"
    public ReadOnlyMemory<byte> Arguments { get; }  // body after the short-string name
}

/// <summary>What the handshake driver receives when the mechanism completes.</summary>
public readonly struct ZMechanismResult
{
    /// <summary>Connection the parser reads traffic on; the raw connection for NULL/PLAIN, a wrapper for CURVE.</summary>
    public IZConnection SessionConnection { get; }

    /// <summary>Owned copy of the peer's READY body; the driver parses Socket-Type from it (0015 section 2.4).</summary>
    public byte[] PeerReadyBody { get; }
}
```

The driver's readiness - when to send local READY - is expressed by the
session itself: the session receives `LocalReadyBody` and writes it at the
right point of its sequence. `PeerReadyBody` is an owned copy because the
scratch is reused by the next read; a per-connection `byte[]` allocation in
the handshake is negligible.

### 3.1 The mechanism surface is fully public

Everything a mechanism needs to implement a ZMTP command sequence is public,
so a mechanism can be written outside the library with no internal access:

- `ZmtpCommandCodec` (public): short-string command-name parsing,
  `ParseMetadata` (the READY/HELLO property format), `ParseErrorReason`, and
  the encode side `MetadataPropertyLength` + `WriteMetadataProperty`.
- `ZmtpCommands` (public): `BuildReady`, `BuildError` - the ERROR a server
  writes when rejecting authentication.
- `ZMechanismContext.WriteCommandAsync` / `ReadCommandAsync` plus
  `LocalReadyBody`, `ZMechanismException`, `ZMechanismResult`.

The extensibility gate (0006 section 4 / section 10 below) is that the next
mechanism slice (PLAIN) is written entirely against this public surface.

## 4. Handshake driver and parser refactor

A new internal `ZmtpHandshake` replaces `ZmtpParser.EstablishAsync` as the
establishment path; `ZmtpParser` becomes traffic-only.

```csharp
// internal, per connection; the role comes from the connection direction and
// is passed to EstablishAsync (outbound => Client, accepted => Server)
internal sealed class ZmtpHandshake(IZConnection connection, IZSecurityMechanism mechanism,
    ReadOnlyMemory<byte> localReadyBody, int maxCommandSize, MemoryPool<byte> pool)
{
    public ValueTask<ZMechanismResult?> EstablishAsync(ZMechanismRole role, CancellationToken token = default);
}
```

Driver sequence:

1. Build the local greeting from `mechanism.Name` and the role's `as-server`
   bit; write it as one gated raw write.
2. Read the peer greeting (64 bytes); validate signature and version (the
   rules currently in `ZmtpParser.ReadGreetingAsync`); extract the mechanism
   field. If it does not equal `mechanism.Name`, send an ERROR command and
   throw `ZeroMqProtocolException` (same pattern as the socket-type mismatch
   today).
3. Run `mechanism.CreateSession(role).RunAsync(context)`; the session drives
   its command sequence and returns the session connection plus the peer
   READY body.
4. Return the result; the socket layer parses the peer Socket-Type and
   validates compatibility, then constructs a `ZmtpParser` over the session
   connection.

Shared codec extraction: `TryReadCommandName`, `ParseMetadata`, and
`ParseErrorReason` move out of `ZmtpParser` into an internal
`ZmtpCommandCodec` used by the parser, the handshake driver, and mechanism
sessions. The fixed `BuildNullGreeting` / `BuildHandshake` members of
`ZmtpFrameEncoder` are replaced by a parameterized internal `ZmtpGreeting`
builder (`Build(name, asServer)` / parse); `WriteGreetingAsync` is dropped
with them (it has no production caller).

`ZSocketBase` touch points:

- `RunConnectionAsync` runs the handshake first, then creates the parser over
  `result.SessionConnection`, sets the frame handler, and only then calls
  `ParseAsync`. No frame can be lost in between: nothing reads the session
  connection between the READY and `ParseAsync`, and the socket buffers.
- The role is derived from the connection direction in `AddConnection`
  (accepted `endpoint == null` => Server).
- `EstablishWithTimeoutAsync` now wraps the handshake instead of the parser;
  the timeout, the incomplete-handshake cap, and the establishment gate are
  unchanged. The handshake's greeting read and the mechanism context's scratch
  rent from the socket pool, so the pool-accounting tests keep their
  invariant that every rental returns on dispose.

### 4.1 Implementation deltas vs. this design

- `ZMechanismContext.ReadCommandAsync` returns a nullable
  `ZMechanismCommand?`; the command name and arguments slices are borrowed
  from the context scratch (valid until the next read).
- The greeting write is the first step of `ZmtpHandshake.EstablishAsync`,
  before reading the peer greeting.
- `ZmtpParser.EstablishAsync` was **removed** (open question resolved: the
  parser is traffic-only; the previously landable "keep as a NULL shim" option
  was rejected for cleanliness since the type is pre-1.0).
- The traffic parser now rents its scratch lazily on the first frame (including
  a zero-length frame), so the first frame in materialization mode needs no
  extra pooling path; the message/queue surfaces allocate each frame directly.

## 5. NULL mechanism

```csharp
public sealed class ZNullMechanism : IZSecurityMechanism
{
    public static ZNullMechanism Instance { get; } = new();
    public string Name => "NULL";
    public IZMechanismSession CreateSession(ZMechanismRole role) => new NullSession();
}
```

Client and server sessions are identical and preserve today's wire behavior:
write `LocalReadyBody` (READY), read the next command, expect READY (capture
its body) or ERROR (throw with the peer's reason); return the raw connection
as the session. Both sides write READY immediately, exactly as the current
code writes greeting+READY and reads the peer's READY.

This is the 0006 section 4 first slice: NULL uses only the boundary, with no
parser special cases and no `as-server` handling beyond the greeting bit.

## 6. PLAIN mechanism (RFC 27)

### 6.1 Wire format

- **HELLO** (client -> server): short-string name "HELLO" plus two
  properties, `Username` and `Password`, each encoded as the READY metadata
  property format (1-byte name length, name, 4-byte big-endian value length,
  value). Values are raw octets, not necessarily UTF-8.
- **WELCOME** (server -> client): short-string name "WELCOME", no properties.
- **ERROR**: as today (0015 parser behavior); the server sends
  "Invalid username or password" on rejection.
- **READY**: the socket-layer command, exchanged after WELCOME as in NULL.

### 6.2 State machines

Client session (after the driver wrote the PLAIN greeting, `as-server = 0`):

1. Write HELLO(username, password).
2. Read command; expect WELCOME (ERROR -> throw with reason; anything else ->
   protocol error).
3. Write local READY.
4. Read command; expect READY, capture its body (ERROR -> throw).
5. Return the raw connection.

Server session (greeting `as-server = 1`):

1. Read command; expect HELLO, parse Username/Password.
2. Authenticate. On rejection, write ERROR("Invalid username or password")
   and throw `ZMechanismException`; the connection is torn down.
3. Write WELCOME, then local READY.
4. Read command; expect READY, capture its body (ERROR -> throw).
5. Return the raw connection.

The post-WELCOME READY exchange is full-duplex and order-tolerant: after
WELCOME both sides write their READY and read the peer's, so either
completion order works.

### 6.3 Credentials and authenticator

A mechanism carries both role configurations (D3):

```csharp
public sealed class ZPlainMechanism : IZSecurityMechanism
{
    /// <summary>Client role: fixed credentials sent in HELLO.</summary>
    public ZPlainMechanism(string username, ReadOnlySpan<byte> password);

    /// <summary>Server role: authenticates HELLO credentials per connection.</summary>
    public ZPlainMechanism(ZPlainAuthenticator authenticator);
}

/// <summary>Server-side PLAIN credential check; a delegate keeps this AOT-safe (no reflection).</summary>
public delegate bool ZPlainAuthenticator(string username, ReadOnlySpan<byte> password);
```

A role whose configuration is absent fails at `CreateSession` (a Server
session without an authenticator rejects every accepted connection), matching
libzmq where PLAIN client and server options are independent
(`ZMQ_PLAIN_USERNAME`/`ZMQ_PLAIN_PASSWORD` vs `ZMQ_PLAIN_SERVER`).

### 6.4 Implementation deltas vs. this design

- The authenticator delegate is `ZPlainAuthenticator(string username,
  ReadOnlySpan<byte> password)`: the username arrives as the decoded UTF-8
  text of the HELLO Username property, and the password as the raw bytes of
  the Password property (the shared metadata parser decodes both as text).
  Password *encoding* is therefore the metadata parser's UTF-8; a caller who
  needs non-UTF-8 passwords would decode through a mechanism-side wrapper.
- The client also parses ERROR: the WELCOME step accepts ERROR and throws
  with the peer's reason, so a server-side rejection surfaces as
  `ZMechanismException` on the client.
- Both roles reject an unexpected first command (the client before HELLO,
  the server before WELCOME) with `ZMechanismException`.

## 7. Configuration

`ZSocketOptions` gains one property; the default preserves today's behavior:

```csharp
public ZSecurityOptions Security { get; init; } = ZSecurityOptions.Null;

public sealed class ZSecurityOptions
{
    public static ZSecurityOptions Null { get; } = new();
    public IZSecurityMechanism Mechanism { get; init; } = ZNullMechanism.Instance;
}
```

Every `ZSocket.Create*` factory forwards `Security` (currently they forward
only `Pool`), and the queue-surface factories copy it onto the underlying
`ZSocketOptions`. The mechanism is resolved once at socket construction -
explicit configuration, no discovery - so it is safe under AOT and
trimming analyzers, which are already enabled with warnings as errors.

## 8. Failure and error contract

- **`ZMechanismException : ZeroMqProtocolException`** covers mechanism
  failures: authentication rejection, an unexpected command, or an ERROR
  command from the peer (carrying the peer's reason). Deriving from
  `ZeroMqProtocolException` means the socket pump's existing catch
  (`ZeroMqProtocolException` faults establishment) handles it unchanged,
  while callers can still distinguish an auth failure.
- **ConnectAsync contract** (0006 section 4): a mechanism failure faults the
  `ConnectAsync` establishment with the mechanism exception; the peer is
  never marked routable and is torn down by the existing pump cleanup. An
  accepted (server-side) rejection surfaces through `PeerEnded` with the
  exception, as peer failures do today.
- **Reconnect eligibility**: failures are per-connection; a new `ConnectAsync`
  starts a fresh handshake. No persistent blacklist is introduced.
- The handshake timeout (0006 section 3.2) covers the whole mechanism
  sequence, so a peer that stalls mid-handshake faults exactly as today.

## 9. CURVE and encryption (deferred)

CURVE is not part of this phase (0015 section 3.3): it requires X25519, a
managed implementation must be evaluated for AOT compatibility and audited
separately, and it is its own tracked item. The evaluation is documented in
0017 - its conclusion is that CURVE ships as an example mechanism with a
user-supplied crypto backend (BouncyCastle for pure managed / AOT), not as a
library built-in. The boundary is ready for it in two ways:

- `ZMechanismResult.SessionConnection` may be a wrapper of the raw
  connection that decrypts on read / encrypts on write; the parser and the
  socket layer are unchanged because they already run against the session.
- The wrapper delegates frame/message sends to the raw connection so the
  write gate and per-message atomicity are preserved.

Encryption that is not a ZMTP mechanism (TLS) stays in the transport layer as
an orthogonal axis (0015 section 3.1); the boundary neither supports nor
needs it.

## 10. Testing

- **NULL behind the boundary**: the existing NULL handshake fixtures
  (`ZmtpTestData.Greeting` / `Ready`) now drive `ZmtpHandshake`; parser tests
  become traffic-only (feed an established connection directly to
  `ParseAsync`). Wire behavior must be unchanged - the NetMQ NULL interop
  matrix is the lock.
- **Replaceability gate (0006 section 4)**: a fake mechanism (e.g. "TEST")
  whose session exchanges a custom command pair (PING/PONG) before READY
  completes a ZmqSharp-to-ZmqSharp handshake through configuration alone.
  Proves the boundary is not NULL-shaped.
- **Extensibility gate (this revision)**: a complete PLAIN mechanism (RFC 27,
  HELLO/WELCOME/READY/ERROR both roles) written only against the public
  surface - compiled in a separate probe project without `InternalsVisibleTo`
  and exercised end to end in `ZmqSharp.Tests.Extensibility`, including an
  authentication rejection faulting the client's `ConnectAsync` with
  `ZMechanismException`. The library's own PLAIN slice must not need any
  internal type this public mechanism cannot use.
- **PLAIN wire fixtures**: HELLO/WELCOME/READY/ERROR in partial-read
  configurations; malformed HELLO (missing property, bad lengths), missing
  Socket-Type in READY, ERROR with a reason - all fail with protocol/mechanism
  errors.
- **Failure contract**: a server-side authenticator rejecting credentials
  faults the client's `ConnectAsync` with `ZMechanismException` carrying
  "Invalid username or password"; an accepted connection rejected by the
  server surfaces through `PeerEnded`.
- **NetMQ PLAIN interop** (extends the 0006 section 5 matrix, both
  directions): ZmqSharp PLAIN client to NetMQ PLAIN server
  (`PlainServer = true`) and the reverse (`PlainUsername`/`PlainPassword`).
  Note: NetMQ 4.0.4.3 (and master) does not implement PLAIN - the greeting
  branch errors with "Not yet supported" and no PLAIN options exist - so
  this bullet is superseded by the scripted wire-contract peer of
  `PlainWireContractTests` (RFC 27 byte-exact, both roles plus rejection).
- **AOT**: no reflection anywhere; the mechanism seam and the authenticator
  delegate keep the existing AOT analyzers clean.

## 11. Milestones

| # | Work item | Size | Notes |
|---|-----------|------|-------|
| 1 | Mechanism boundary + NULL extraction | Medium | **Implemented** (this revision): 0006 section 4 gate met; parser slims to traffic; `ZmtpHandshake` + `ZmtpCommandCodec` + `ZmtpGreeting`; READY Socket-Type validation moved to the socket layer; codec made public and the PLAIN-style mechanism verified end to end against the public surface only |
| 2 | PLAIN mechanism | Small | **Implemented** (this revision): `ZPlainMechanism` + `ZPlainAuthenticator`, pure command frames, no crypto (0015 section 3.3); built only against the public mechanism surface and verified by wire fixtures plus end-to-end real-socket handshakes incl. an authentication rejection |
| 3 | PLAIN wire contract | Small | **Implemented** (this revision) as a *scripted* raw-TCP peer: NetMQ implements no PLAIN (4.0.4.3 and master both error at the greeting with "Not yet supported" and expose no PlainServer/PlainUsername/PlainPassword options), so no NetMQ PLAIN peer exists. `PlainWireContractTests` speak RFC 27 byte-for-byte from the spec - HELLO/WELCOME/ERROR bodies and the as-server bit - and assert what our library sends and accepts, in both roles plus the rejection path. The ZMTP framing/command layers they build on are already locked by the NetMQ NULL interop suite. A NetMQ PLAIN peer would require upstream NetMQ work; revisit if NetMQ lands one |
| 4 | CURVE (example mechanism, user crypto backend) | Large | **Evaluated in 0017**: not a built-in; ships as a protocol-skeleton example over a user-chosen X25519 backend |

The security boundary depends on nothing else in 0015, so slice 1 can land in
parallel with 0015 item 1 (dispatch/type split) and item 3 (write-path
cluster). This design keeps `core.SocketTypeName` and
`IsCompatibleSocketType` until 0015 section 2 lands; the handshake driver is
the single place that later switches to `ZSocketType.AcceptsPeer`.

## 12. Rejected alternatives

- **Command pump in the driver** (the libzmq
  `process_handshake_command` model): the driver reads each command and
  dispatches it to the session, which returns the next action. Rejected: the
  session-driven model is simpler, matches 0015 section 3.2's "the mechanism
  runs its own handshake sequence", and keeps protocol state inside the
  mechanism where it belongs.
- **Reflection-based discovery** ("instantiate for the peer's mechanism
  field"): rejected - prohibited by 0006 section 2.5 and the AOT stance;
  replaced by D1 (configured instance matched by name).
- **Multiple mechanisms per socket**: rejected - ZMTP allows one mechanism
  per connection; combination freedom comes from the orthogonal axes
  (socket type x transport encryption x mechanism x dispatch policy, 0015
  section 3.1), not from stacking.
- **Mechanism owns the greeting**: rejected - signature/version/mechanism
  matching stays in one place (the driver); mechanisms contribute only their
  `Name` and command sequence.
- **Parser keeps the handshake with a pluggable step**: rejected - violates
  0015 section 3.2 ("the parser never sees it") and the 0006 completion gate
  ("NULL uses only the mechanism boundary rather than parser special cases").

## 13. Open questions

- **Namespace**: resolved by 0018 - the mechanism surface ships as a dedicated `ZmqSharp.Security` layer, separate from the ZMTP wire codec layer in `ZmqSharp.Zmtp`.
- **`ZmtpParser.EstablishAsync`**: removed (a draft public type; the parser is
  traffic-only). Resolved in implementation (section 4.1).
- **as-server conflict strictness**: the current code ignores the peer's
  `as-server` bit and interops fine with NetMQ. This design writes and reads
  the bit but does not enforce "exactly one server" - worth revisiting when
  PLAIN interop lands, since libzmq's PLAIN path is more sensitive to it.
- **PLAIN password encoding**: RFC 27 treats credentials as octets; whether
  the client API should take `string` (UTF-8) or bytes is a usability call
  for slice 2.
