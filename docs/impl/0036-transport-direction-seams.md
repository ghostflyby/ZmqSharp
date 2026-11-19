# 0036 - Transport Direction Seams

Status: accepted
Date: 2026-11-19

Refines the transport family in 0034.

## 1. Why the combined factory was wrong

0034 gave `IZTransport<TSelf, TEndpoint>` both `ConnectAsync` and
`BindAsync`, so every transport had to invent the direction it did not
support. The stacked ZWS client transport is the first in-box case that
genuinely cannot listen; the next likely cases are the mirror image
(listener-only transports such as an in-proc accept endpoint or a
server-only WebSocket listener). The combined interface also forced test
doubles to implement a throwing `BindAsync` or a no-op listener surface.

The inventory at the time of this change:

- In-box transports: `SocketTransport` connects and listens; this is the
  only in-box transport today. A client-only transport arrives in the
  stacked ZWS change.
- Test transports: eight implement the combined interface: five are
  connect-only and three loopback/controlled fakes need both directions.
  Two of the connect-only fakes carried a throwing `BindAsync` placeholder;
  the rest returned a no-op listener.

"Both directions" is therefore the exception, not the default, and the
interface should let an implementation declare the direction it supports.

## 2. The model

The shipped non-generic `IZTransport` is renamed to the listening instance
surface `IZServerTransport` (`OnAccept`, `StartAsync`, `Dispose`). Two
factory seams sit on top of it, and no combined type exists:

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

`SocketTransport` implements both interfaces explicitly (the only in-box
full-duplex transport); a client-only or listen-only transport implements
one. Code that needs both directions declares both constraints:

```csharp
where TTransport : IZClientTransport<TTransport, TEndpoint>,
                    IZServerTransport<TTransport, TEndpoint>
```

The runtime constraints follow the direction of the operation:
`ConnectAsync`/`Disconnect` require `IZClientTransport`;
`BindAsync`/`Unbind` require `IZServerTransport`.

## 3. Compatibility

Both `IZTransport` and `IZTransport<TSelf, TEndpoint>` are shipped (0.8.0),
so the rename and removal are breaking changes accepted for 0.9.0.
`PublicAPI.Unshipped.txt` records the removed entries with the
`*REMOVED*` prefix; the release housekeeping strips that prefix and deletes
the corresponding lines from `Shipped` when the tag ships. The 0.8.x
`OnAccept` signature stays in `Shipped` and is removed by the same
mechanism; the new `IZServerTransport.OnAccept` is an Unshipped addition.
The ledger also now records the earlier 0.9.0 removals (`IZConnection.Abort`,
`ZmtpParser`, `ZmtpFrameEncoder`) that had been dropped from `Shipped`
without removal entries. An existing implementation of the combined
interface now lists both narrow interfaces (a source change, not a
behavioral one); `SocketTransport` already does.

## 4. Consequences

- A transport declares exactly the direction it supports; the other
  direction is absent at compile time, not a throwing placeholder.
- Test doubles shrink to the factory they use (the connect-only fakes now
  implement `IZClientTransport`; a listen-only fake covers the server
  seam; the bidirectional loopbacks list both interfaces).
- There is no combined `IZTransport<TSelf, TEndpoint>` type; code that
  needs both directions declares the two constraints, which keeps the
  public surface minimal and the requirement explicit.
- `SocketTransport` is the only in-box bidirectional transport and is the
  model for a full-duplex implementation.
- The string endpoint facade in this change still accepts only the
  tcp/ipc schemes; a stacked transport change can add a scheme without
  touching the direction seams.

## 5. Related infrastructure

The same change extracts `ZMessageSession` from `ZmqProtocol` unchanged, so
a native message transport reuses the established session logic without a
second implementation. It also adds the internal `IZProtocolCarrier` seam:
a connection that negotiated its protocol in the transport supplies it to
the runtime, while an explicit `ZSocketOptions.WireProtocol` still wins.
The carrier is internal to the built-in transports; external transports
select a protocol through `ZSocketOptions.WireProtocol`.
