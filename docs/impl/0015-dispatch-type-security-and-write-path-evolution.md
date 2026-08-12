# 0015 - Dispatch, Type Declaration, Security, and Write-Path Evolution

Status: draft
Date: 2026-08-12
Revision: 1

Consolidates the design review of the transport and pattern layers into a
forward plan. It captures four decisions that the current code already
motivates, plus one explicit deferral:

- **Dispatch vs. socket identity** must be split: `IPatternCore` mixes
  behavior (`RouteOutbound`) with identity (`SocketTypeName`), and the
  round-robin code of `ZDealerCore` and `ZPushCore` is already duplicated
  verbatim, proving the concepts are distinct.
- **Security mechanisms** must be pluggable, not hard-coded to NULL.
- **The connection shape** should not be forced through `Stream`; the
  write path needs a sink abstraction for multi-segment atomic writes.
- **`ipc://` (Unix domain sockets)** closes the one gap in the libzmq
  transport set and is nearly free because `IZTransport` is endpoint-agnostic.
- **Streaming messages** are deferred (section 7).

This document is a plan, not a design for one feature: sections 2-6 each
define a work item with its decisions and acceptance, and section 8 orders
them. It extends 0007 (transport core / pattern core / surface) and 0006
(interop matrix, feature checklist).

## 1. Problem

Five observations from the current implementation converge on one direction:

1. **Identity and behavior are fused.** `IPatternCore` (0007 section 2.2)
   both routes outbound messages and advertises a socket type. The duplicated
   `ZDealerCore` / `ZPushCore` bodies (`ZPatternCore.cs`) show that round-robin
   is a reusable policy, not an identity. Socket-type compatibility is a
   hard-coded switch in `ZSocketBase.IsCompatibleSocketType`, so no custom
   socket type can participate.
2. **Security is a constant, not a plug-in.** The greeting's mechanism field
   is validated against `NULL` only, and the handshake hard-codes
   "expect READY next". The `IZConnection` doc already promises that
   "no handshake is built in... so security mechanisms can vary freely", but
   the parser does not honor that promise.
3. **The write path is stream-shaped and fragmented.** `ZmtpFrameEncoder`
   writes each segment with its own `Stream.WriteAsync` (one system call per
   segment), and a multi-segment frame is not written atomically. The default
   connection wraps a `NetworkStream`, hiding the socket underneath.
4. **`ipc://` is missing.** Every maintained ZMTP implementation supports it;
   the transport set has only TCP.
5. **The message layer cannot express lazily-produced frames**, so large
   results must be materialized fully in memory before sending.

## 2. Dispatch policy and socket-type declaration

### 2.1 Split the pattern core

`IPatternCore` becomes two independent seams:

```csharp
// Outbound selection only: a reusable, neutral policy.
public interface IZDispatchPolicy
{
    IZConnection? RouteOutbound(ZMessage message, ReadOnlySpan<IZConnection> peers);
}

// Identity only: the advertised Socket-Type and the peer-accept predicate.
public sealed class ZSocketType
{
    public required string Name { get; init; }                 // advertised in READY
    public required Func<string, bool> AcceptsPeer { get; init; }
}
```

Concrete dispatch policies are named for what they do, not for one socket
type:

- `ZRoundRobinDispatch` - DEALER, PUSH (and REQ's fair-queue send across
  connections, where the current-connection gate applies).
- `ZSinglePeerDispatch` - PAIR.
- `ZBroadcastDispatch` - PUB, XPUB.
- `ZIdentityDispatch` - ROUTER.
- `ZCurrentPeerDispatch` - REQ's single current connection.

### 2.2 Compatibility is per-endpoint, not derived

ZMTP socket-type compatibility is asymmetric (REQ accepts REP, PUSH accepts
PULL, PUB accepts SUB), so a peer's type cannot be derived from the local
type. Each socket type declares its own `AcceptsPeer` predicate; the built-in
eleven keep the existing matrix from `ZSocketBase.IsCompatibleSocketType`.

### 2.3 Custom socket types: scope of interop

A custom socket type interoperates only between ZmqSharp endpoints: the peer
must advertise the same name string. Standard names (the ZMQ set) interoperate
with libzmq/NetMQ. Consequently a custom type cannot be validated by the
NetMQ interop suite; it is validated with in-library pair tests only.

### 2.4 Touch points

`SocketTypeName` has exactly two consumers that move with the split:

- the READY handshake (`BuildReady`), which broadcasts the local name;
- the peer validation in the connection pump, which checks the peer's READY
  against the local `AcceptsPeer`.

The refactor is behavior-preserving for the built-in types; the compatibility
matrix must be locked by the existing interop tests.

## 3. Security mechanisms

### 3.1 "Arbitrary combination" means orthogonal layers

ZMTP allows exactly one mechanism per connection (chosen in the greeting), so
mechanisms do not stack with each other. Combination freedom lives in the
four orthogonal layers:

```text
socket type (section 2) x transport (encryption, e.g. TLS) x mechanism (authentication) x dispatch policy
```

Encryption belongs in the transport (a TLS transport wrapping TCP), not in a
mechanism. Mechanism = authentication, per ZMTP's design.

### 3.2 Mechanism as a connection transform

A mechanism is an `IZConnection -> IZConnection` transform; the parser never
sees it. Handshake flow:

1. Read the peer's greeting mechanism field.
2. Instantiate the configured mechanism for that field.
3. The mechanism runs its own handshake sequence on the raw connection
   (NULL: expect READY directly; PLAIN: HELLO -> WELCOME -> READY; custom:
   any command sequence).
4. The mechanism returns a session connection (decrypt-on-read /
   encrypt-on-write for CURVE), and the parser parses traffic on the session.

Public seam: `IZSecurityMechanism` (name + handshake state machine) plus
per-mechanism credential objects declared in socket options.

### 3.3 Cost split

- **PLAIN is cheap**: pure command frames, no crypto. Near-term target.
- **CURVE is a large work item**: requires X25519. Native libsodium conflicts
  with the zero-native-dependency / AOT stance; a managed implementation
  (e.g. BouncyCastle) must be evaluated for AOT compatibility and audit
  separately. CURVE is its own tracked item, not part of this phase.

## 4. Dedicated socket connection

`NetworkStream` has no internal buffering: `ReadAsync` copies directly into
the caller's buffer, so the "multiple reads per frame" loop is unavoidable on
a raw socket too. The win is not fewer reads; it is removing the wrapper and
the `Stream` virtual-call layer, and unlocking the write path of section 6:

- a `ZSocketConnection(Socket) : IZConnection` (internal) that reads directly
  with `Socket.ReceiveAsync(Memory, flags, token)`;
- `SocketTransport` returns it when the underlying endpoint is a raw socket;
- `ZConnection(Stream)` stays for generic transports (extension seam);
- the per-connection write gate is retained: message-level atomic writes
  still require serialization.

Zero API cost: `ZConnection` is already internal.

## 5. ipc / Unix domain sockets

`ipc://` in ZMTP is a Unix domain socket (path addressing). .NET has
`UnixDomainSocketEndPoint`; AF_UNIX is supported on Windows 10 1803+.

### 5.1 No new transport type

`IZTransport<TSelf, TEndpoint>` is endpoint-agnostic by design, and
`SocketTransport` already implements the `EndPoint` endpoint. Passing a
`UnixDomainSocketEndPoint` to the generic `ConnectAsync`/`BindAsync` must
work, with two fixes in `SocketTransport`:

- `new Socket(SocketType.Stream, ProtocolType.Tcp)` hard-codes the
  address family (InterNetwork); construct from the endpoint instead:
  `new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified)`.
- `NoDelay = true` is TCP-only; set it only when
  `endpoint.AddressFamily != AddressFamily.Unix`.

### 5.2 Extension parsing

`ZSocketExtensions` gains an `ipc://path` scheme mapping to a
`UnixDomainSocketEndPoint` (plus unlink-on-dispose for the bound path).

### 5.3 Differentiation

libzmq and NetMQ support `ipc` on Unix only. On Windows, ZmqSharp is likely
the first ZMTP-compatible library to offer `ipc://` - a concrete differentiator
worth documenting in the README once implemented.

### 5.4 Testing

Parameterize the existing TCP suites (`[Theory]` over transport) so PAIR
echo, REQ/REP, etc. run once per transport. CI matrix note: NetMQ's ipc
transport is Unix-only, so Windows interop cannot use NetMQ for ipc; use
ZmqSharp-vs-ZmqSharp on Windows and NetMQ interop on Unix.

## 6. Write path: sink + ReadOnlySequence + PredictSize

### 6.1 Sink abstraction

The encoder writes frames to a sink, not a stream:

```csharp
public interface IZWriteSink
{
    ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask WriteAsync(ReadOnlySequence<byte> sequence, CancellationToken token = default);
}
```

The encoder produces each frame (header + all segments) as one
`ReadOnlySequence` and hands it to the sink in a single logical write:

- the socket sink uses buffer-list scatter writes (`Socket.SendAsync` with a
  buffer array), preserving the frame's segment structure with one system
  call;
- the stream sink writes the segments sequentially (the current behavior)
  but inside one gate acquisition;
- a future lazy-segment source (section 7) feeds the same sequence channel.

This fixes the N+1 system calls and the non-atomic multi-segment write.

### 6.2 Two-phase encoding and PredictSize

Two-phase encoding (compute the size, then write) is the standard way to
enable pooled allocation. The ZMTP frame header is deterministic - 2-byte
short header at length <= 255, 9-byte long header above - so the encoded size
is exactly predictable, not just a lower bound. The seam offers both:

- `PredictMinimum` - the lower bound, for a cheap "does this fit the scratch
  buffer" check;
- `PredictSize` - the exact/upper bound, for renting a buffer that is written
  exactly once with no growth.

### 6.3 What "custom encoder" means

The ZMTP frame and command encoding is protocol-fixed and not customizable.
What is customizable is (a) the write sink (where frames go) and (b) a
user-side DTO -> `ZMessage` serialization layer (which is where a user would
want PredictSize and streaming). The second is a separate layer from the ZMTP
encoder and is not part of this work item.

## 7. Streaming messages: deferred

### 7.1 What "streaming" can mean here

A ZMTP frame header must precede the body with its length, so a
"length-unknown stream" cannot be one frame. There are only two cases:

- **Total length known, segments produced on demand** - a streamed message.
  This is what a streaming API would express.
- **Total length unknown** - just split into multiple messages and send each;
  the existing async send already supports this, so no new API is needed.

Streamed messages matter only when a large logical message needs atomicity
(REQ/REP reply, ROUTER identity frames) and cannot be split.

### 7.2 Use cases after mmap

Memory-mapped files already cover the file case at zero extra memory, so
streaming adds nothing there. The real remaining cases:

1. **Transform pipelines**: compress/encrypt/transcode "on the fly" - a 1 GB
   log compressed while being sent has no on-disk transform product to mmap.
   Strongest case.
2. **Incrementally-produced sources**: log tail, live telemetry, another
   process's stdout.
3. **Atomic huge messages**: a REQ/REP or ROUTER reply that must be one
   message (identity/delimiter frames cannot be split) without materializing
   tens of GB.
4. **Low-memory/embedded targets**: AOT scenarios where no contiguous pool
   buffer is large enough.

### 7.3 Encoding in the send context

Encoding a lazy source during the send is correct: the connection's message
write gate already serializes the whole send, and ZMTP forbids interleaving on
one connection, so partial-frame interleaving cannot corrupt the peer. The
real costs are:

- the connection is held for the whole send (inherent to the frame protocol);
- cancellation mid-send leaves a half frame on the wire, so the connection
  must be torn down - there is no clean resume;
- ownership: the streaming source (file handle, generator, channel) must
  outlive the send, which the 0007 move rules already express ("one live
  copy").

### 7.4 What stays now

Nothing in the library changes. Section 6's `ReadOnlySequence` write path is
the reserved channel; a future "lazy segment source" (an
`IAsyncEnumerable<ZSegment>`-style frame producer) plugs into it without
touching the encoder.

## 8. Milestones and priority

Adopted from the review discussion (section 7 is deferred):

| # | Work item | Section | Size | Notes |
|---|-----------|---------|------|-------|
| 1 | Dispatch/type split | 2 | Medium | Zero protocol risk; unblocks custom types and the neutral policy names |
| 2 | ipc + parameterized tests | 5 | Small | Two fixes in `SocketTransport`; clear differentiator |
| 3 | Write-path cluster: sink + socket connection + PredictSize | 6, 4 | Medium | One cluster; the sink is the shared seam |
| 4 | PLAIN mechanism | 3 | Small | Pure command frames, no crypto |
| 5 | CURVE mechanism | 3 | Large | Own tracked item; managed X25519 AOT evaluation first |
| 6 | Streaming messages | 7 | Large | Deferred; only the `ReadOnlySequence` channel is reserved now |

Ordering rationale: 1 first because the socket-type declaration is the base
for custom sockets and touches the handshake; 2 is cheap and visible; 3
depends on neither but is the largest visible performance change; 4+ then
security; 6 is a standalone deferred item.

## 9. Rejected alternative: BCL Pipelines

The transport shapes resemble `System.IO.Pipelines` (`PipeReader`/`PipeWriter`
are the Kestrel-style primitives for parsers and encoders). The option was
reviewed and **rejected**: `PipeReader` hands out `ReadOnlySequence`s over the
pipe's internal buffer, and materializing frames from that buffer into the
target adds a copy - the original reason Pipelines was discarded. The plan
keeps the hand-written zero-copy path and adopts only Pipelines' *shape*:
a sink abstraction, prefetch (GetMemory-equivalent), and multi-segment writes
(section 6), all as library-owned types rather than BCL pipes.

This is recorded so the evaluation does not need to be repeated.
