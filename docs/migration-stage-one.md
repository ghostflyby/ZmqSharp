# Stage-One API Migration

The changes described by implementation document 0029 intentionally break API
compatibility. Update core and CURVE packages together.

## Endpoint shutdown

`UnbindAsync` and `DisconnectAsync` now return `ValueTask`, accept a cancellation
token, and finish only after the target listener/peer has cleaned up:

```csharp
using ZmqSharp;

await client.DisconnectAsync("tcp://localhost:5555", cancellationToken);
await server.UnbindAsync("tcp://localhost:5555", cancellationToken);
```

Use the same string used at registration. Teardown does not perform another DNS
lookup. Canceling a wait after shutdown begins does not undo shutdown. Unbind
preserves accepted peers; Disconnect affects outbound peers. Unknown endpoints
are successful no-ops.

To initiate shutdown from a targeted peer's callback without awaiting that
callback's own completion, use the generic synchronous operation:

```csharp
client.Disconnect<EndPoint, SocketTransport>(resolvedEndpoint);
server.Unbind<EndPoint, SocketTransport>(resolvedEndpoint);
```

These advanced calls additionally import `System.Net` and `ZmqSharp.Transports`.
Do not await `DisconnectAsync` or socket disposal from a callback that those
operations need to finish. Code that requires a `Task` can call `AsTask()` once;
prefer direct `await` for normal usage.

Custom `IZSocket` implementations implement generic synchronous teardown and all
four string operations in addition to the existing generic methods. Lifecycle
fields/helpers previously inherited through `ZAsyncState` now belong to
the internal SocketRuntime behind ZSocketBase (0030); there is no replacement
standalone lifecycle base. See migration-frame-sessions.md for custom subclasses.

String endpoint operations are now instance methods. `ZSocketExtensions` is
removed; ordinary `socket.BindAsync(...)` calls keep the same syntax. Replace
explicit static calls to the extension facade with instance calls. Endpoint
parsing now belongs to the internal `ZEndpointParser`.

## Requests and subscriptions

`RequestAsync` still returns `Task<ZMessage>`. Cancellation also covers the wait
for the reply and retires the selected peer, preventing a late reply from being
used by a subsequent request. Reconnect or select another peer before continuing.
The request task completes after the send stops accessing borrowed data, even
when canceled or failed. Dispose successful reply messages exactly once.

```csharp
byte[] topic = "news"u8.ToArray();
subscriber.Subscribe(topic);       // copies the registered prefix
subscriber.Unsubscribe("news"u8);  // content match; span input is supported
```

Mutating `topic` after Subscribe does not change the filter or reconnect messages.
An empty span subscribes to all topics. Code requiring the exact old array method
signature must adapt to the span signature.

## Receive diagnostics and CURVE backends

Consumers can classify a rejected receive through public root types:

```csharp
socket.PeerEnded += (_, failure) =>
{
    if (failure is ZReceiveRejectedException rejected)
        Console.WriteLine($"{rejected.Rejection.Reason}: {rejected.Rejection.Actual} > {rejected.Rejection.Limit}");
};
```

Custom `ICurveCryptoBackend` implementations must add
`DerivePublicKey(ReadOnlySpan<byte> secretKey, Span<byte> publicKey)` for X25519.
The default backend checks a 32-byte secret and at least 32 destination bytes.
Existing `Key32`, message construction factories, and configuration defaults
remain available. The default backend still brings the BouncyCastle package.
