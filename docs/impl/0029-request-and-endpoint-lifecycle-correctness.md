# 0029 - Request and Endpoint Lifecycle Correctness

Status: draft
Date: 2026-10-07

Stage-one behavior specification for 0028. The implementation and regression
tests are present; draft status is retained pending design acceptance. This
revision supersedes endpoint teardown semantics in 0002 and request completion
semantics in 0010 where they differ.

## 1. Endpoint operations

`IZSocket` retains generic `Task`-returning Bind/Connect and adds:

```csharp
void Unbind<TEndpoint, TTransport>(TEndpoint endpoint);
void Disconnect<TEndpoint, TTransport>(TEndpoint endpoint);
ValueTask UnbindAsync<TEndpoint, TTransport>(TEndpoint endpoint,
    CancellationToken token = default);
ValueTask DisconnectAsync<TEndpoint, TTransport>(TEndpoint endpoint,
    CancellationToken token = default);
```

The generic constraint remains `TTransport : IZTransport<TTransport, TEndpoint>`.
String Bind/Connect return `Task`; string Unbind/Disconnect return `ValueTask`
and accept optional cancellation. The string methods are available on the common
interface and socket base. Endpoint parsing is an internal socket helper; the
redundant string extension methods and their public facade type are removed.
String shutdown has no synchronous overload because address setup can involve DNS.

Synchronous teardown requests stopping, without awaiting pumps or consumer
callbacks. Async teardown also awaits the target registration's completion:
listener loop exit or peer pump exit, internal cleanup, end notification, and
resource release. Calling an awaited teardown from a targeted peer's own callback
would await that callback itself; use the generic synchronous operation there.

Registrations own resource shutdown, a linked cancellation source, and a
completion signal. They remain registered during shutdown, so repeated calls
share completion even after the peer leaves the routable snapshot. Cancellation,
resource release, and awaits happen outside the socket state lock. Listener and
connection registration occurs before background work is published for disposal.
Rejected resources are disposed outside the lock, including a bind factory
returning after the socket has closed.

Identity combines the transport type with endpoint equality. String registrations
also retain the original string and resolved endpoint: teardown matches the
original string with ordinal equality and does not resolve DNS again. Different
spellings are not aliases. Unknown endpoints complete successfully. A strongly
typed teardown can target a string registration by its resolved endpoint.

Unbind stops matching listeners and preserves accepted peers. Disconnect stops
all matching outbound registrations, including an in-progress registered
handshake. A pre-canceled teardown token initiates nothing. Once shutdown starts,
canceling the caller only cancels its wait; shutdown continues.

## 2. Request outcomes and ownership

`RequestAsync` remains `Task<ZMessage>`. Each request owns its target peer,
outcome/completion sources, cancellation registration, and send lifetime. The
core receives peer selection, borrowing directed send, and retirement capabilities rather
than depending on `ZReqSocket`.

Only one reply, send failure, parse failure, cancellation, or peer exit wins the
outcome. No continuation identifies an operation solely by the current pending
slot: it holds the original request state. Reply parsing failures fault that
request before propagating the protocol error to peer teardown. Losing replies
are disposed.

The public task and in-flight slot finish only after the send stops using its
message and cancellation callbacks finish. This holds for success, cancellation,
and failure, protecting borrowed caller buffers. Precondition failures occur
before the core takes ownership; successful admission transfers the message to
the request core, which disposes the framed message after the borrowing send
capability completes. The reply returned to the caller is owned by the caller.

Cancellation retires the selected connection before releasing the request slot;
late replies from that connection cannot satisfy a future request. Send failures
also retire the target, avoiding reuse after a possibly partial write. The caller
must reconnect or use another peer to continue. Failure and cancellation must
complete pending tasks even when there is no reply. Transport disposal leaves
managed write gates alive for an in-flight writer's `finally` release.

## 3. Other public changes

- SUB/XSUB `Subscribe` and `Unsubscribe` take `ReadOnlySpan<byte>`. Subscription
  storage copies prefixes. Broadcast/reconnect use owned copies, an empty prefix
  matches every topic, and unsubscription matches content.
- `ZReceiveRejectedException`, `ZReceiveRejection`, and
  `ZReceiveRejectionReason` are public root types. `PeerEnded` exposes the
  configured limit, observed value, and classification through the exception.
- Lifecycle fields and helpers move from `ZAsyncState` to `ZSocketBase` while
  retaining protected capabilities. `ZAsyncState` is removed.
- `ICurveCryptoBackend.DerivePublicKey(ReadOnlySpan<byte>, Span<byte>)` derives an
  X25519 public key. The default implementation requires a 32-byte secret and at
  least 32 destination bytes. The mechanism does not call BouncyCastle directly;
  the existing backend package dependency remains.

## 4. Validation and migration

Regression tests use controlled completion signals for fake transport cleanup,
request sending, and cancellation; TCP/IPC exercise actual endpoint behavior and
subscription reconnects. Test cancellation before and after shutdown starts,
repeated shutdown, transport identity, handshake interruption, absent string
addresses, malformed replies, losing replies, and borrowed/pooled buffer release.
Verify the backend seam with a recording implementation and a known public-key
vector, then retain interoperability and allocation tests.

Run Release build, format verification, full xUnit/FluentAssertions suites, and a
Native AOT publish/run of a consumer using core and CURVE. Keep warnings as errors.

Consumers storing teardown as `Task` must await `ValueTask` directly or use
`AsTask()` once. Custom `IZSocket` implementations must implement the new sync and
string operations. Derived sockets use lifecycle members on `ZSocketBase`.
Custom CURVE backends must implement public-key derivation. Array subscription
call sites still convert to spans but may no longer change registered prefixes
by mutating the input. See `docs/migration-stage-one.md` for examples.

## 5. Implementation validation (2026-10-08)

Validated locally on macOS arm64 with .NET SDK 10.0.401:

- Release solution build: zero warnings and errors.
- Full-solution `dotnet format --verify-no-changes --no-restore`: passed.
- Complete test run: 376 passed, none failed or skipped, including allocation
  measurements, interoperability, and the new request/endpoint/subscription cases.
- Native AOT consumer publish with warnings as errors and execution: passed.
  The consumer exercised PAIR and CURVE exchanges, string shutdown, span
  subscriptions, public rejection types, and request cancellation.
- Implementation-document numbers are unique; the untracked legacy sample
  directories contained only build artifacts and were removed.

The existing CI matrix remains responsible for Windows and Linux validation.
Stage-two and stage-three changes remain the roadmap in 0028.
