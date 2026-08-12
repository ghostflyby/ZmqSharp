# 0001 - Message Model and API Layering

Status: draft
Date: 2026-08-06

This document fixes the foundation design of a ZMTP-style .NET messaging
library: API layering, message model, queue semantics, and memory ownership
rules. ZMTP parsing details, the pattern layer, and routing are covered in
later documents.

## 1. Goals

A ZMTP / ZeroMQ-style messaging library for the modern .NET runtime.

Non-goals:

- Not an RPC framework.
- No automatic retry.
- No business-level correlation.
- No hidden business-level timeout policies.

## 2. Layered Model

```text
Application
  |
  +-- ZQueueSocket<TSocket>  high layer: queue API (Channel), per-peer queues, takes over the low-level callback at construction (0002)
  +-- IZCallbackSocket low-layer primitive: borrowed frame callback (0002)
  +-- ZMTP Session     per-peer connection: greeting / handshake / traffic
  +-- Transport        TCP / IPC / inproc (pluggable)
```

The message surface spans two layers:

- Low layer: borrowed `ZFrame` streamed to the callback, zero allocation, valid only during the call.
- High layer: owned `ZMessage` (sealed class), may escape; the consumer is responsible for Dispose.

## 3. Key Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | No `System.IO.Pipelines` | A message protocol is inherently streaming and frame lengths are known up front, so renting buffers on demand is sufficient; Pipe's retain/Advance semantics create a borrowed-vs-contiguous conflict, and its model is a poor fit for non-pure-streaming applications |
| D2 | Low-layer struct view + high-layer class wrapper, single move | Zero-allocation, inlinable hot path; ownership transfers once at the boundary, no reference counting needed |
| D3 | Multipart preserved, frames are first-class | Routing envelopes, topics, and REQ/REP delimiter frames depend on frame boundaries (RFC 23) |
| D4 | Contiguity is per-frame and consumer-driven | Frame structure (protocol semantics) is orthogonal to memory layout (performance); contiguity implies materialization |
| D5 | Atomic send/receive | RFC 23: all frames of a message or none |
| D6 | Pooled/owned per segment; no Detach | The standard pool abstractions expose no escape hatch; permanent ownership must be decided before allocation |
| D7 | Backpressure belongs to the Channel | `capacity` = HWM; pause with hysteresis resume when full; drop configurable |
| D8 | Receive policy is extensible | Fixed options by default; a v2 application-level Decide hook |

## 4. Low-Level Callback Contract

```csharp
public readonly struct ZFrame
{
    public ReadOnlyMemory<byte> Memory { get; }
    public bool More { get; }
}

public delegate bool ZFrameHandler(ZFrame frame, CancellationToken token);
// true  = keep receiving
// false = pause the receive pump (backpressure), resumed via Resume()
```

Rules:

- Streaming: the callback is invoked once per frame as it is read; a multipart
  message arrives as consecutive frames until `More` is false. No upfront
  assembly is required, so consumers can process frames incrementally.
- Borrowed: a frame's memory is valid only during the call; references must not
  be kept (the parser reuses its scratch buffer).
- Synchronous and serialized; never concurrent.
- `false` only means "pause"; dropping messages is a high-layer Channel policy, never mixed into the low-level bool.

## 5. High-Level Queue Socket

```csharp
public sealed class ZQueueSocket<TSocket> : IAsyncDisposable   // defined in 0002
{
    // BoundedChannelOptions.Capacity = per-peer HWM
    public ChannelReader<IZMessage> Messages { get; }
    public void Complete(Exception? error = null);
}
```

- Subscribes to the low-level frame callback at construction; per peer, the
  parser materializes each frame directly into its final pooled/owned buffer
  (0004 constraint 1), assembles `ZMessage` / `ZMultiMessage` at the last
  frame, and writes the per-peer queue (0004).
- Full: `TryWrite` fails -> callback returns false (pause); a background resumer waits on `writer.WaitToWriteAsync()` and resumes with a low-watermark hysteresis at `Count <= capacity / 2` to avoid thrashing at the boundary.
- Drop mode (for PUB-like lossy semantics) is an explicit option, off by default.
- Protocol errors -> `writer.TryComplete(exception)`, visible to consumers.
- On close/completion the Channel is drained and all unconsumed messages are disposed to prevent pool leaks.
- Watermarks, reader/writer concurrency are fully decided by the Channel; the library adds no extra semaphores.

## 6. Message Model

```csharp
public interface IZMessage : IReadOnlyList<ReadOnlySequence<byte>>, IDisposable
{
    // Count, this[int], GetEnumerator(): frames of the message.
    bool TryGetContiguousFrame(int index, out ReadOnlyMemory<byte> memory);
    bool TryGetOwnedArray(int index, out byte[] array); // owned, single-segment (0003)
}

public sealed class ZMessage : IZMessage          // single frame
{
    public static ZMessage FromOwned(byte[] data); // zero copy, Dispose never touches a pool
    public void Dispose();                         // idempotent
}

public sealed class ZMultiMessage : IZMessage      // multipart, one buffer per frame
{
    public int Count { get; }
    public void Dispose();                         // idempotent
}
```

Invariants:

- Storage is a struct array of segments; each segment holds an `object Owner`
  that is either the `byte[]` itself (owned) or an `IMemoryOwner<byte>`
  (pooled). The two boundary classes are the only custom reference types.
- Contiguous means a frame does not span segments. Frames are contiguous by
  default; a single frame may span segments (non-contiguous).
- Multi-segment `ReadOnlySequence` values are built on demand over the BCL
  `ReadOnlySequenceSegment` facility; nodes are transient and never stored.
- `Dispose` is idempotent (guarded by `Interlocked`) and returns Pooled
  segments; Owned segments (the `byte[]`) are only dropped.

## 7. Receive Pipeline (No Pipe)

```text
Socket.ReceiveAsync -> parser -> materialization policy -> per-peer queue -> socket-type aggregation
```

- Contiguous frames: the header carries the size, so rent a pooled buffer of exactly that size, fill it (ReadExactly semantics), and deliver a single segment. The delivered buffer is the final buffer; there is no second copy (0004 constraint 1).
- Segmented frames: rent fixed blocks (8KB suggested), chained into a `ReadOnlySequence<byte>`.
- `ContiguousFrameLimit` defaults to 85,000 (LOH threshold): frames up to the limit are contiguous; larger frames are segmented (stay off the LOH); frames over 1MB (the `ArrayPool<byte>.Shared` 2^20 cap) are forced segmented.
- No Pipe: its model targets streaming parsers that retain unconsumed bytes; a message protocol knows lengths up front, so renting on demand is simpler. Under pure-streaming semantics, segmentation vs contiguity is only a performance parameter.
- The pooled/owned choice per message is the receive policy (0003).

## 8. Send Path

- `SendAsync(ReadOnlyMemory<byte>)`: the library copies (like `zmq_msg_init_buffer`); caller data is unconstrained.
- `SendAsync(ZMessage)`: ownership transfers; the library disposes after sending (like `zmq_msg_init_data`).
- Single writer per connection; a message is an atomic unit; any frame write failure or peer close terminates the connection and disposes in-flight messages; never deliver half a message.

## 9. Connection and Reconnect

- Lifecycle: connect -> greeting -> handshake -> traffic -> disconnect.
- Reconnect produces a new ZMTP connection with a fresh greeting / handshake.
- Unexpected disconnect is a temporary error; reconnect with randomized backoff to avoid connection storms (RFC 23 Error Handling).
- An ERROR command is fatal: close the connection and do not reconnect with the same credentials.

## 10. Receive Policy and Extension Points

The low-level callback streams every frame as it is read into a reusable
scratch region (borrowed, valid during the callback) via `ZFrameHandler`.
Multipart messages arrive as consecutive frames until `More` is false. Queue
materialization is a separate concern (0003): the parser reads each frame
directly into its final pooled/owned buffer (0004 constraint 1) and writes the
assembled message into the per-peer queue. Backpressure: the callback's false
pauses the receive pump; a full per-peer queue pauses only that peer.

## 11. Engineering Constraints

The authoritative list lives in AGENTS.md. Highlights: xUnit tests with FluentAssertions; no `!` null-forgiving operator (`is {}` pattern matching instead); collection literals; `System.Lock`; fully async pipeline; latest C# style; full AOT support.

## 12. Project Structure and Namespaces

- Project split: v1 keeps a single library (`ZmqSharp`) plus a test project (`ZmqSharp.Tests`); no multi-library split.
  Rationale: the library has zero external dependencies, and a single assembly keeps AOT/trimming simplest; layering is enforced by namespaces and dependency direction.
  Split triggers: a second transport implementation (e.g. TLS/QUIC), or a second consumer of the protocol layer.
  When split: `ZmqSharp` (core: message model, session, parser, patterns) + `ZmqSharp.Transports` (implements `IZTransport`), with dependency direction Transports -> Core.
- Namespaces by layer: the top-level `ZmqSharp` namespace (base public API: messages, sockets, configuration) plus `ZmqSharp.Zmtp` (ZMTP wire codec) / `ZmqSharp.Transports` / `ZmqSharp.Security` (security mechanisms). The full organization is specified in 0018.

## 13. Follow-ups

- The pattern layer (routing, REQ/REP state machine) gets its own document.
- 0002 defines the socket architecture; 0003 the receive policy; 0004 the
  per-peer queue model and performance constraints.
- Open questions: connection-level heartbeat (PING/PONG, ZMTP 3.1) and streaming consumption APIs for very large frames (>1MB).
