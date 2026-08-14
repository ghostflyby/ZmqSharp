# ZmqSharp

A fully asynchronous, AOT-compatible .NET implementation of the ZMTP 3.0 protocol (ZeroMQ wire protocol) with
libzmq-compatible socket semantics.

- **Value-type messages** (`ZMessage` / `ZFrame` / `ZSegment`) with explicit ownership and move semantics — no
  per-message allocations on the hot path.
- **All 11 libzmq socket types**: PAIR, PUSH, PULL, PUB, SUB, REQ, REP, DEALER, ROUTER, XPUB, XSUB, each verified
  against the NetMQ (libzmq-compatible) implementation over TCP in both directions.
- **TCP and ipc (Unix domain socket) transports** with a single string-endpoint facade (`tcp://host:port`,
  `ipc://path`). libzmq supports ipc on Unix only, and NetMQ's `ipc://` is a TCP hash rather than a Unix domain
  socket; ZmqSharp offers true `ipc://` Unix domain sockets on every platform, including Windows — a concrete
  differentiator (0015 section 5.3, 0020 section 7/8).
- **Per-peer bounded queues** with configurable full modes (Wait, DropWrite, DropNewest, DropOldest) and mandatory
  reclamation of dropped messages.
- **Copy-on-write peer snapshots**: zero-allocation send and receive hot paths in optimized builds.
- **Full Native AOT**: no runtime reflection or dynamic code generation.

## Usage

```csharp
using ZmqSharp;

// PAIR over TCP.
await using var server = new ZQueueSocket<ZPairSocket>(new ZPairSocket());
await using var client = new ZQueueSocket<ZPairSocket>(new ZPairSocket());
await server.BindAsync("tcp://127.0.0.1:5555");
await client.ConnectAsync("tcp://127.0.0.1:5555");

await client.SendAsync("hello"u8.ToArray());
var message = await server.Messages.ReadAsync();

// PAIR over ipc (Unix domain socket): an absolute path, a relative one
// resolved against the system temp directory, or - on Linux - an abstract
// namespace name (libzmq's ipc://@name convention) that creates no
// filesystem entry.
await using var ipcServer = new ZQueueSocket<ZPairSocket>(new ZPairSocket());
await using var ipcClient = new ZQueueSocket<ZPairSocket>(new ZPairSocket());
await ipcServer.BindAsync("ipc:///tmp/zmqsharp.sock");
await ipcClient.ConnectAsync("ipc:///tmp/zmqsharp.sock");
```

REQ/REP (operation-oriented):

```csharp
await using var rep = new ZRepSocket();
rep.BindRequestHandler((context, token) =>
    rep.SendReplyAsync(context, ZMessage.FromOwned("pong"u8.ToArray()), token));

await using var req = new ZReqSocket();
var reply = await req.RequestAsync(ZMessage.FromOwned("ping"u8.ToArray()));
```

## Design

Design documents live in `docs/impl/`. The architecture is a transport core (`ZSocketBase`) composed with per-pattern
cores and bound to a semantic delivery seam (`IPatternSink`); surfaces are thin composition roots constructed directly
(`new ZPairSocket()`, `new ZQueueSocket<ZPairSocket>(new ZPairSocket())`), with `BindAsync`/`ConnectAsync` as the
repeatable endpoint surface (0022).

## License

Apache-2.0 — see [LICENSE](LICENSE). Third-party notices in [NOTICE](NOTICE).
