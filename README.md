# ZmqSharp

A fully asynchronous, AOT-compatible .NET implementation of the ZMTP 3.0 protocol (ZeroMQ wire protocol) with
libzmq-compatible socket semantics.

- **Value-type messages** (`ZMessage` / `ZFrame` / `ZSegment`) with explicit ownership and move semantics — no
  per-message allocations on the hot path.
- **All 11 libzmq socket types**: PAIR, PUSH, PULL, PUB, SUB, REQ, REP, DEALER, ROUTER, XPUB, XSUB, each verified
  against the NetMQ (libzmq-compatible) implementation over TCP in both directions.
- **Per-peer bounded queues** with configurable full modes (Wait, DropWrite, DropNewest, DropOldest) and mandatory
  reclamation of dropped messages.
- **Copy-on-write peer snapshots**: zero-allocation send and receive hot paths in optimized builds.
- **Full Native AOT**: no runtime reflection or dynamic code generation.

## Usage

```csharp
using ZmqSharp.Sockets;

// PAIR over TCP.
await using var server = ZSocket.CreatePair();
await using var client = ZSocket.CreatePair();
await server.BindAsync("tcp://127.0.0.1:5555");
await client.ConnectAsync("tcp://127.0.0.1:5555");

await client.SendAsync("hello"u8.ToArray());
var message = await server.Messages.ReadAsync();
```

REQ/REP (operation-oriented):

```csharp
await using var rep = ZSocket.CreateRep();
rep.BindRequestHandler((context, token) =>
    rep.SendReplyAsync(context, ZMessage.FromOwned("pong"u8.ToArray()), token));

await using var req = ZSocket.CreateReq();
var reply = await req.RequestAsync(ZMessage.FromOwned("ping"u8.ToArray()));
```

## Design

Design documents live in `docs/impl/`. The architecture is a transport core (`ZSocketBase`) composed with per-pattern
cores and bound to a semantic delivery seam (`IPatternSink`); surfaces are thin composition roots created through
`ZSocket.Create*` factories.

## License

Apache-2.0 — see [LICENSE](LICENSE). Third-party notices in [NOTICE](NOTICE).
