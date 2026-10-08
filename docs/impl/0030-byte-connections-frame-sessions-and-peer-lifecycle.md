# 0030 - Byte Connections, Frame Sessions, and Peer Lifecycle

Status: accepted
Date: 2026-10-08

Implements the replacement model authorized after the reviews in 0028 and 0029.
Public domain namespaces and ordinary socket construction remain unchanged.
Breaking extension API changes are migrated directly, without obsolete adapters.

## 1. Why the previous design existed

The early connection abstraction combined transport I/O, frame encoding, message
serialization and receive callbacks. It gave pattern implementations one object
for sending and identifying peers, and gave security mechanisms an apparent
`connection -> connection` extension point. This was convenient while NULL and
PLAIN returned the raw connection unchanged.

The model confused two distinct forms of encryption. TLS transforms a byte
stream; CURVE authenticates ZMTP frames, including their logical flags. To fit
CURVE into a byte connection, the wrapper decoded a complete ciphertext frame,
reconstructed a plaintext wire header and shifted its payload, then the parser
parsed that reconstructed stream again. That extra work followed from the
boundary, rather than from the CURVE specification.

The queue surface inherited another historical assumption: a wrapper owns its
own peer queues. Moving that wrapper into the socket preserved a second registry,
second lifecycle bookkeeping and public-event-dependent cleanup. Separate send
and receive snapshots were useful; separate owners were not.

The other incorrect assumption was that bind selects the security server and
connect selects the security client. Security role is configuration; connection
origin controls accepting limits and endpoint shutdown. The NULL role bit must
always be zero. Sources: [ZMTP 3.0](https://rfc.zeromq.org/spec/23/),
[ZMTP CURVE](https://rfc.zeromq.org/spec/25/),
[CurveZMQ](https://rfc.zeromq.org/spec/26/).

These are responsibility and ownership problems within an assembly. The aim is
not to eliminate every cross-domain type reference or to rename public domains.

## 2. Model and ownership

```text
Socket surface
  -> selection policies / pattern coordinator
  -> SocketRuntime
       -> Dictionary<ZPeer, PeerRecord>
            -> endpoint registration, phase, cancellation, completion
            -> byte connection
            -> ZmtpSession: message send gate, parser, encoder, optional frame codec
            -> materializer, message accumulator
            -> optional queue and per-peer read lock
       -> send candidate snapshot / receive queue projection
```

`ZPeer` is a root-namespace sealed identity with an internal constructor and a
read-only diagnostic `Id`. Equality is reference identity. Reconnect creates a
new identity. It exposes no I/O or disposal and is unrelated to ROUTER wire ids.
Events, request contexts, sinks and selection policies take `ZPeer`.

`PeerRecord` owns resources throughout
`Registered -> Handshaking -> Established -> Stopping -> Closed`.
Records are registered before starting the handshake, so Disconnect can find
handshaking connections. Send selection can include those peers; sending waits
for their establishment gate. Entering Stopping removes both published views,
cancels the gate and rejects new send registrations. The record remains in the
single registry until final cleanup. The queue projection stores references to
these records and owns no peer dictionary.

## 3. Byte contract and message serialization

`IZConnection : IZByteReader, IZByteWriter, IDisposable` provides only byte
reads, memory/sequence writes and `Abort()`.

- Reads may be partial; zero means EOF.
- Successful writes consume the entire input. TCP loops over partial single and
  scatter sends; stream transports iterate segments within one logical write.
- One reader and one writer can run concurrently. The connection does not lock
  individual segments or own protocol encoding.
- Abort is idempotent and requests I/O termination immediately. It does not
  imply that pending operations have finished. Dispose releases remaining
  resources after operations finish and ensures abort.
- Sequence nodes, input memories and backing arrays remain valid until the
  write completes, including after cancellation has requested an abort.
- `ZTransportOptions`, `IZWriteSink`, `IZMessageSink`, frame/message sends and
  callback registration are removed from the byte transport surface.

The internal `ZmtpSession` holds a message-level send gate across all multipart
frames. Its encoder computes LongSize and writes header plus body through one
sequence write. Reusable sequence nodes avoid allocating a chain per frame.
Cancellation before obtaining the gate does not retire the peer. Failure or
cancellation after entering the write phase retires it, preventing another
message from following a truncated frame stream.

## 4. Frame codec and receive path

The public ZMTP extension consists of `ZmtpFrameData` and `IZFrameCodec`:

```csharp
public readonly struct ZmtpFrameData
{
    public ZmtpFrameFlags Flags { get; init; }
    public ReadOnlySequence<byte> Body { get; init; }
}

public interface IZFrameCodec : IDisposable
{
    ZmtpFrameData Encode(ZmtpFrameData logicalFrame);
    ZmtpFrameData Decode(ZmtpFrameData wireFrame);
    long GetMaximumEncodedLength(long logicalBodyLimit);
}
```

Views are borrowed. Outputs stay valid until the next call in the same direction.
Encode and decode have independent state and buffers and may run concurrently;
each direction is serialized. Flags contain More and Command, never LongSize;
Command cannot combine with More. NULL/PLAIN return no codec.

`CurveFrameCodec` owns per-connection seal, decrypt and optional gather buffers.
It transforms one frame, authenticates before delivering flags/payload, rejects
replayed nonces and validates logical flags. It returns decrypted payload memory
following the authenticated flags byte. It never synthesizes a plaintext wire
header, shifts payload to make space for one, or invokes a second parser.

`ZmtpParser` takes a byte reader, explicit asynchronous callback and optional
codec. The clear path reads into final owned/pooled materialization segments.
The encrypted path bounds the ciphertext before reading it, authenticates it,
then applies logical frame/message/frame-count guards through the materializer.
Because logical command flags are encrypted, the pre-read bound uses the maximum
of the configured frame limit and command limit, plus the codec's expansion.
Commands also receive their logical command limit after authentication.

Borrowed delivery can expose the decrypt buffer directly, including across an
awaited callback or pause. Owned/pooled delivery copies once into final segments.
Synchronous OnFrame borrowing remains valid only during the call. It is not
interchangeable with the asynchronous `ZFrameHandlerAsync` contract.

## 5. Security and handshake

`IZSecurityMechanism.Role` is fixed by socket mechanism configuration;
`CreateSession()` takes no role argument. None/Client/Server are explicit:
NULL uses None and emits zero; PLAIN/CURVE constructors configure client or
server. Client bind and server connect are supported. Peer role-bit differences
remain tolerated for reference implementation interoperability.

Handshake orchestration lives in `ZmqSharp.Sockets`, separate from wire encoding
and parsing. The public `ZMechanismContext` exposes local metadata and command
read/write only. It does not expose the raw connection. A successful result
contains an owned peer metadata view and optional codec; codec ownership passes
to the session. A mechanism that fails before returning owns its partial codec.

Timeout/cancellation aborts the raw connection and awaits the actual handshake
operation before releasing context scratch. No abandoned WhenAny loser keeps
using a disposed context. The incomplete accepted-handshake limit counts accepted
connections only, independently of their configured security role.

## 6. Shutdown and background sends

Under the state lock, mark stopping and withdraw publication. Outside it:

1. Cancel the registration and abort byte I/O.
2. Await handshake/receive completion and all registered sends.
3. Notify internal coordinators, drain queued messages under the peer read lock,
   and dispose accumulated frames.
4. Await cancellation callbacks; dispose parser, codec/session, connection and
   registration cancellation resources.
5. Publish PeerEnded; in finally complete the exit signal and remove the record.

Owned background sends release their message before ending their send lease.
SUB replay and XPUB forwarding observe failures, retire failed peers and participate
in shutdown through those leases. No unobserved send task owns message buffers.
Internal cleanup does not subscribe to public events. A throwing public event can
fault the exit task, but cannot skip cleanup or strand its completion signal.

Endpoint semantics from 0029 remain: synchronous generic calls initiate shutdown;
asynchronous calls await cleanup, repeated calls share completion, pre-cancel
starts nothing and later cancellation cancels only that caller's wait. Unbind
preserves accepted peers. String teardown uses saved registrations and does not
resolve DNS again. A callback cannot await an exit that includes itself.

## 7. Coordinator construction and queue components

Composition roots create the runtime, handler slot and coordinator before exposing
a socket. REQ receives snapshot, borrowed-send and retirement delegates. REP receives
handler and owned-send delegates. XPUB receives snapshot and tracked forwarding.
There is no socket back-reference, post-construction Attach or InboundPolicy cast.
Dispatch/type/inbound remain useful decision fragments; REQ/REP/XPUB are bidirectional
stateful coordinators, not three independent policies.

Receive mode and queue configuration are resolved once in the runtime constructor,
with explicit surface capabilities. No base-constructor virtual probe is needed.
`AggregateReader`, `WakeGate` and `ReceiveMaterializer` are directly testable internals.
Readers and queue reclamation share the record's read lock. Wake capture precedes
the readable-level check so a producer signal cannot be lost between check and wait.

## 8. Implementation stages and evidence

The changes were validated in the requested sequence:

1. Fixed roles/greeting: 380 tests passed after the 376-test starting baseline.
2. Byte connection/session/CURVE codec: the same 380 tests passed.
3. Unified runtime/queue/coordinators: direct component and lifecycle tests added,
   followed by wire/interop/allocation acceptance tests.

Deterministic gates cover an unfinished borrowed write at Abort, handshake cleanup
that deliberately waits after cancellation, and a throwing public event after
resources are released. Existing REQ cancellation/late-reply and endpoint suites
continue to cover TCP, IPC and fake transports. CURVE is tested against NetMQ
4.0.4.3 in all four local role/bind combinations; in-library CURVE exchange covers
normal and reversed roles over TCP and real IPC.

Allocation gates measure send+encode, decode, and the complete borrowed parser+
decode path after warmup on a fixed thread. Whole-parser measurement exposed
HasFlag boxing and a new borrowed-memory wrapper; bit checks and direct backing
owner borrowing eliminate them. Array/memory-manager borrowed segments retain
their existing ownership semantics and never dispose the backing owner.

Real socket measurements use an isolated Release process, 64-byte borrowed single
frames, callback reception, 1,000 warmup frames and five batches of 10,000 frames.
`GC.GetTotalAllocatedBytes(true)` covers both I/O directions and BCL async work.
The same harness runs against archived pre-change HEAD and the working tree.
Median process bytes per delivered message on this macOS ARM64/.NET 10.0.401 run:

| Path | Before | After |
| --- | ---: | ---: |
| TCP NULL | 431.15 | 79.06 |
| TCP CURVE | 262.42 | 88.40 |
| IPC NULL | 299.17 | 75.16 |
| IPC CURVE | 86.45 | 73.40 |

These are comparative measurements, not zero-allocation network guarantees: BCL
completion scheduling varies between batches. Deterministic allocation tests enforce
library hot-path budgets. The performance harness is retained at
`eng/measure-wire-allocations/` for repeating the measurements against another checkout.

Release build, full tests, format verification and Native AOT publish/run are
required final checks; section 9 records their results.

## 9. Final validation

The following results were recorded locally on macOS ARM64:

- Release solution build: zero warnings and errors, with warnings-as-errors.
- Full xUnit/FluentAssertions suite, including allocation gates: 412 passed.
- Solution and both engineering harnesses pass format verification.
- Native AOT published and executed on osx-arm64: AOT-SMOKE-OK. The smoke
  assembly has no access to internals and implements a public mechanism/codec
  and a byte-only transport adapter; it exchanges NULL and reverse-topology
  CURVE over TCP and IPC.
- The published native executable passed all four PLAIN role/topology exchanges
  against pyzmq/libzmq 4.3.5. No sleeps are used to establish ordering.

The reference check identified an additional existing standards defect: the old
PLAIN HELLO encoded credential metadata and omitted INITIATE. Corrected RFC 24
wire fixtures now test length-prefixed credentials, binary passwords, octet size
limits and HELLO/WELCOME/INITIATE/READY. The legacy fixtures had been mislabeled
as RFC 27, which specifies ZAP rather than the PLAIN wire mechanism.

### Automated external consumer validation

`consumer-validation.yml` exposes both `workflow_call` and `workflow_dispatch`.
CI calls it for pull requests, pushes to main and manual CI runs. The existing
release requirement for a successful CI run on the same commit therefore includes
this validation. Published-package smoke remains a separate manual workflow.

The consumer job runs on Ubuntu and publishes the public-extension smoke as
`linux-x64` Native AOT with warnings-as-errors. It installs the native build
prerequisites, uses Python 3.12 with pyzmq 27.1.0, runs NULL and reverse-topology
CURVE over TCP/IPC, and checks all four PLAIN role/bind combinations with that
same native executable. It also builds the allocation measurement consumer and
verifies formatting of both engineering projects; performance measurements remain
manual. The job has a 20-minute timeout and uploads its logs regardless of outcome.

The validation has run in CI: on pull request #41 and on main at commit
`767974e` (2026-10-08). The AOT consumer printed `AOT-SMOKE-OK`, the PLAIN
harness passed all four role/bind combinations, and all three platform jobs
reported 412 tests with zero failures. Linux AOT and PLAIN acceptance therefore
rest on a recorded workflow run. The local macOS evidence above is retained
independently.
