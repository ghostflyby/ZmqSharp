# 0021 - Dedicated Socket Connection and Write Sink

Status: accepted
Date: 2026-08-13
Revision: 1

Implements 0015 section 4 (the dedicated socket connection) and section 6.1
(the write sink), the connection-shaped half of work item #3. 0015 section
6.2 (PredictSize / two-phase encoding) is tracked separately and is not
implemented here.

## 1. Problem

Every socket connection went through `new ZConnection(new NetworkStream(socket, true))`:
the parser read via `NetworkStream.ReadAsync` (a pure pass-through over
`Socket.ReceiveAsync`), and the encoder wrote each frame segment with its own
`Stream.WriteAsync` - one system call per segment, a multi-segment frame never
written atomically, and the whole hot path buried under the Stream virtual-call
layer.

The win is not fewer reads (0015 section 4): `NetworkStream` has no internal
buffering, so the parser's read-exactly loop is unchanged on a raw socket too.
The win is the wrapper removal plus the write path of section 6.1.

## 2. The write sink

The encoder now writes frames to a sink, not a stream (0015 section 6.1):

```csharp
internal interface IZWriteSink
{
    ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default);
    ValueTask WriteAsync(ReadOnlySequence<byte> sequence, CancellationToken token = default);
}
```

`ZmtpFrameEncoder` produces each frame (header + all segments) as one
`ReadOnlySequence<byte>` and hands it to the sink in a single logical write.
The public `ZmtpFrameEncoder(Stream)` constructor is preserved: it wraps the
stream in an internal `StreamWriteSink` that writes the sequence's segments
sequentially - byte-identical to the previous encoder, so the existing
encoder unit tests pass unchanged and `ZConnection(Stream)` (the
generic-transport extension seam) is untouched.

`ReadOnlySequence` has no `(Memory, Memory)` constructor, so the common
two-part form (header + one body) costs a transient two-node chain (~96 B of
gen-0 garbage per frame - comparable to the scatter send's BCL `Task<int>`
(section 4)). This is outside the measured allocation gates (they use fake
transports that bypass the encoder and connection).

## 3. `ZSocketConnection(Socket)`

`ZSocketConnection : IZConnection` (internal) replaces the `NetworkStream`
wrapper on the socket transport:

- reads directly with `Socket.ReceiveAsync(buffer, SocketFlags.None, token)`;
- writes a multi-segment sequence with one buffer-list scatter send
  (`Socket.SendAsync(IList<ArraySegment<byte>>, SocketFlags)`), gathering the
  frame's segments into a reused list; array-backed segments go in place
  (the rule: header plus owned/pooled segment memories), non-array-backed
  (native) memory falls back to a single pooled copy;
- keeps the per-connection `SemaphoreSlim` write gate - message-level atomic
  writes still require serialization (0015 section 4);
- `Dispose` closes the socket directly, which aborts a pending
  `ReceiveAsync` - the `DisconnectAsync` scenario that disposing a stream
  could not reliably interrupt (0006 section 3.6).

`SocketTransport` returns `ZSocketConnection` from both `ConnectAsync` and the
accept loop. `ZConnection(Stream)` stays for generic transports (0015 section
4: extension seam).

## 4. Measured write cost: scatter vs. alternatives

The scatter overload is the only multi-buffer `SendAsync` in .NET 10
(no `ReadOnlySequence` / `ReadOnlyMemory[]` overloads exist), and it returns a
BCL `Task<int>` while the Memory-based overloads return `ValueTask<int>`. A
loopback benchmark (50k sends, 128-byte body, Release) measured the trade:

| Path | Syscalls/frame | BCL alloc/frame | µs/frame |
|------|---------------|-----------------|----------|
| Scatter `IList<ArraySegment<byte>>` (chosen) | 1 | ~72 B `Task<int>` | 1.45 |
| Two separate `Memory` sends | 2 | 0 | 2.02 |
| One coalesced `Memory` send (copy) | 1 | 0 | 1.17 |

The syscall dominates: a second send costs ~0.57 µs against ~0.28 µs for the
72 B of gen-0 garbage (the real cost of which is the GC pressure it causes at
high message rates, not the allocation itself). The two-send option is the
worst path (slowest, doubled syscalls and wire segments) and was rejected.
The scatter path keeps zero-copy frame segments, matching 0015 section 6.1's
design. The coalesced number is the theoretical floor the SAEA path would
reach with zero allocation (section 7).

### 4.1 Where the ~72 B comes from, and why the scatter overload cannot be cancelled

The scatter overload is the .NET 4.5-era `SocketTaskExtensions` surface over
`SocketAsyncEventArgs`: `Socket.Tasks.cs` takes the socket's **cached**
`TaskSocketAsyncEventArgs` (`Interlocked.Exchange` on a per-socket field,
returned on completion) - so the SAEA itself is reused, not allocated - and
returns `Task.FromResult(...)` on the synchronous-completion path. That
freshly-created completed `Task<int>` is the ~72 B. The Memory-based
overloads instead use `AwaitableSocketAsyncEventArgs : IValueTaskSource`,
which completes synchronously as a struct `ValueTask` with no `Task` at all,
which is why they measure 0 B.

The absence of a `CancellationToken` is not an oversight but the API's
historical shape. `SocketAsyncEventArgs` (the high-performance .NET 2.0/3.5
design, still what servers like Kestrel build on) has no cancellation
concept: once `SendAsync(e)` returns true the operation is submitted and can
only be aborted by closing the socket. The 4.5 Task wrapper inherited that,
and no token-taking scatter overload has ever been added. Cancellation of an
in-flight `IList` send is therefore defined as socket teardown - which is
exactly what `ZSocketConnection.Dispose` performs. The write gate is what
makes the per-socket cached SAEA reusable in the first place: two concurrent
`IList` sends on one socket would fall through to allocating a fresh SAEA
(the runtime keeps only one cached).

## 5. Testing

- The transport-parameterized suites (ZSocketTests, ZReqRepTests,
  ZPlainMechanismTests, CustomSocketTypeTests, InboundPolicyTests,
  DispatchPolicyTests, ZSocketIpcTests) run every scenario over
  `ZSocketConnection` on both tcp and ipc: the primary regression surface.
- New `ZSocketConnectionTests`: a raw-socket pair with the parser pump in the
  background, asserting byte-exact frame round-trips (single frame, long
  frame, segmented multi-segment frame over the scatter path) and the direct
  read path.
- New encoder tests use a `CaptureSink` to lock the 0015 section 6.1
  invariant: each frame is exactly one logical write of header + all
  segments, with the segment structure preserved and the MORE flag in the
  header byte.
- The allocation measurement project is unaffected: its fake transports
  bypass the encoder and connection, so the `count + 2` receive baseline and
  the send-path allocation-free gate hold as before.

## 6. Deferred

- 0015 section 6.2 (PredictSize / two-phase encoding) - separate work item.
- 0015 section 7 (streaming messages) - deferred as designed; the sequence
  channel is the reserved plug point.
- Zero-allocation scatter (section 7) - recorded here, not implemented.

## 7. Future: zero-allocation scatter

The ~72 B/frame comes from the completed `Task<int>` the `IList` overload
returns (section 4.1); the SAEA underneath is already per-socket cached. A
`SocketAsyncEventArgs` with a `BufferList` plus a custom `IValueTaskSource`
wrapper would give one syscall, zero copy, zero allocation - reaching the
1.17 µs coalesced baseline with scatter semantics, and dropping the
~96 B/frame encoder chain (two sequence nodes) as a bonus. This mirrors what
Kestrel-class servers do. It is recorded as the next write-path step, sized
separately.

## 8. Acceptance

- Full solution Release build green with `TreatWarningsAsErrors` and the AOT
  analyzers (no runtime reflection or dynamic code generation added).
- `ZmqSharp.Tests` 287/287, `ZmqSharp.AllocationTests` 5/5 (baseline
  unchanged), `ZmqSharp.Security.Curve.Tests` 8/8.
- `dotnet format --verify-no-changes` clean.
- 0015 work item #3's connection half (§4 + §6.1) is complete.
