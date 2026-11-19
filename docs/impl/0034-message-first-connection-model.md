# 0034 - The Message-First Connection Model

Status: accepted
Date: 2026-11-16

Supersedes the connection split in 0033 (its framing/protocol layering
stands; its two sibling connection currencies and the duplicate transport
family do not).

## 1. Why the split was wrong

0033 made the message connection a sibling of the byte connection and kept a
parallel `IZMessageTransport` family. The runtime then had two of everything:
`BindAsync`/`ConnectAsync` and `BindMessageAsync`/`ConnectMessageAsync`,
parallel accept/connect cores, parallel `Add*Connection` paths, and duplicated
unbind/disconnect methods that differed only in their generic constraint. A
byte transport could never be consumed where the runtime needed messages, so
the socket had to remember which family each endpoint used.

The split also put framing in the wrong place. The runtime adapted a byte
connection *after* the transport created it, so the connection could not own
its dialect, a custom framing silently missed the receive composition
(allocator, bound, message-boundary callback), and every protocol author had
to thread a second nullable byte-origin parameter through establishment.

## 2. The model

One connection contract, message-first:

```csharp
public interface IZMessageConnection : IDisposable
{
    ValueTask<ZWireUnit?> ReadUnitAsync(CancellationToken token = default);
    ValueTask WriteMessageAsync(ZMessage message, CancellationToken token = default);
    ValueTask WriteCommandAsync(ReadOnlyMemory<byte> body, CancellationToken token = default);
    bool RetainsMessages { get; }
    void Abort();

    // The raw byte origin for a byte-backed connection; null for a native one.
    IZConnection? ByteOrigin => null;

    // Post-establishment receive composition; native connections own their
    // frames and ignore it.
    void ConfigureReceive(in ZReceiveComposition composition) { }
}

public interface IZConnection : IZMessageConnection, IZByteReader, IZByteWriter
{
}
```

A byte connection *is* a message connection. `IZConnection` is the byte
specialization: it adds the raw I/O plus the inherited message surface. A
native message transport implements `IZMessageConnection` directly and leaves
`ByteOrigin` null; ZWS is the in-box example (0035).

### Shared implementation

`ZByteConnection` is the shared base for byte transports:

- implementers override only `ReadAsync`, both `WriteAsync` overloads, and
  `Abort` (plus `DisposeCore` when the byte resource needs cleanup);
- the base owns the framing view that converts messages to bytes and back,
  including the command path and the receive rebind;
- the view is created lazily with `ownsConnection: false`, so disposing or
  aborting the connection never recurses through the view;
- `ConfigureReceive` forwards the runtime's post-establishment composition to
  the view; the view is unbounded until then, because limits are composition
  policy, not a property of a standalone connection.

The shared state is in the connection instance (or its view): the framing
scratch, recycled owners, headers, send staging, allocator, and bound. There
is no static cache and no per-call framing object.

### Framing and composition

The framing dialect is chosen per socket (`ZSocketOptions.Framing`, default
`ZmtpFraming`) and travels to the transport factory in `ZTransportContext`
(framing and pool). The transport constructs its connection with that context,
so a connection owns exactly one message view. A custom framing over bytes is
selected the same way; a native message transport ignores the framing, and the
runtime rejects a non-default framing on a connection without a byte origin.
The command bound is not part of the context: the runtime installs it on the
connection before the handshake and replaces it with the traffic composition
afterwards.

```csharp
public interface IZFraming
{
    IZMessageConnection Adapt(
        IZConnection connection, MemoryPool<byte> pool, bool ownsConnection = true);
}
```

`ZReceiveComposition` replaces the old concrete `ZmtpFramingConnection.Rebind`
downcast: the runtime calls `ConfigureReceive` after establishment on every
connection, and a framing that materializes frames applies the allocator,
encoded bound, and message-boundary callback in place. Native connections
ignore it.

### One transport family

0036 refines this section: connect and listen are separate seams, and the
combined generic interface is gone.

```csharp
public interface IZServerTransport : IDisposable
{
    event Func<IZMessageConnection, CancellationToken, ValueTask>? OnAccept;
    ValueTask StartAsync(CancellationToken token = default);
}

public interface IZServerTransport<TSelf, in TEndpoint> : IZServerTransport
    where TSelf : IZServerTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<TSelf> BindAsync(
        TEndpoint endpoint, ZTransportContext context, CancellationToken token = default);
}

public interface IZClientTransport<TSelf, in TEndpoint>
    where TSelf : IZClientTransport<TSelf, TEndpoint>
{
    static abstract ValueTask<IZMessageConnection> ConnectAsync(
        TEndpoint endpoint, ZTransportContext context, CancellationToken token = default);
}
```

A full-duplex transport implements both interfaces (`SocketTransport` is
the in-box example); one-direction transports implement one.

`IZMessageTransport` is gone. A byte transport returns its `IZConnection`
(the cast is safe: it is a message connection); a native transport returns its
native connection. The socket has one `BindAsync`/`ConnectAsync` pair, one
accept path, one connection record, and one unbind/disconnect family. The
byte origin is read from the connection (`ByteOrigin`) instead of threading a
second parameter through establishment.

## 3. Consequences

- The runtime composes the establishment command bound explicitly before the
  handshake and the traffic composition after it; a standalone connection
  stays unbounded.
- Establishment is a single `IZWireProtocol.EstablishAsync` call; there is no
  one-shot establisher object.
- The session owns the codec send path's assembled message: the caller's
  input is never transferred when a codec encodes a new message, and the
  assembled message is released unless the connection retains it.
- Receive limits are validated on every delivered message, so native and
  custom connections get the same numeric limits as the framing's
  pre-allocation guard.
- The framing and the runtime share one recycled-buffer pool implementation
  (`ZRecycledBufferPool`), released when the connection or socket disposes.
- The codec send fast path resolves the staged writer through the connection
  or its framing view (`ZStagedWrites`), never through a concrete framing type
  check.
- A byte-transport implementer derives `ZByteConnection`; an implementer that
  cannot derive can still implement `IZMessageConnection` directly and return
  it from the transport.
- Framing is per socket, not per bind call: one socket speaks one dialect.
- `IZConnection` gains the message members and the transport factory changes
  currency, both breaking changes accepted for 0.9.0; the
  message/framing/protocol surface was still unshipped.

## 4. Invariants preserved

- One logical write per frame; scatter/gather untouched.
- Zero-copy receive via the policy allocator, staged scratch otherwise.
- The recycled-owner pool and its RENTED/POOLED/DEAD lifecycle.
- Retention: byte-backed views copy (`RetainsMessages` false), native
  transports may retain; the runtime's fan-out copy/dispose logic is unchanged.
- Command units are single-frame control messages; the framing stays
  codec-blind.
- A failed read releases accumulated frames; a failed establishment releases
  the connection.
