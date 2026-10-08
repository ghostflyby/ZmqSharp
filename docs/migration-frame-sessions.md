# Byte/Frame Boundary and Peer Migration

Implementation design 0030 intentionally changes extension APIs. Upgrade the core
and CURVE package together. Ordinary socket construction, queue reads, message
ownership, request return types and endpoint shutdown syntax remain available.

## Custom transports

Implement only IZConnection's byte reads, both whole-input writes, Abort and
Dispose. Read may return partial data or zero at EOF. A sequence write must not
return until every segment has been written, including partial socket sends.
Only one writer and reader are active at once; message serialization is provided
by the runtime session. Keep borrowed input valid through actual I/O completion.
Abort requests termination immediately and is idempotent. Dispose releases final
resources after outstanding operations finish and ensures abort.

Transport factories receive endpoint and optional cancellation token, without
ZTransportOptions. Remove SendAsync/SendFrameAsync/SendCommandAsync and receive
callback registrations from transport implementations. IZWriteSink and IZMessageSink
are removed. ZmtpFrameEncoder accepts IZByteWriter; ZmtpParser accepts IZByteReader,
ZFrameHandlerAsync and optional IZFrameCodec explicitly.

## Security mechanisms

Add Role to IZSecurityMechanism and implement parameterless CreateSession(). Role
is fixed by socket configuration, independently of bind/connect. NULL uses None;
PLAIN/CURVE credentials/authenticator constructors select Client or Server.
A client can bind, and a server can connect; accepting limits use network origin.

ZMechanismContext no longer exposes Connection. Use its local READY metadata and
command read/write capabilities. Return owned peer metadata and optional Codec in
ZMechanismResult. NULL/PLAIN return null Codec; CURVE returns CurveFrameCodec.
Do not return a wrapped byte connection or reconstruct decrypted wire frames.
A mechanism owns a codec until successfully returned, then the session owns it.

Implement IZFrameCodec.Encode/Decode/GetMaximumEncodedLength and Dispose. Inputs
and outputs are borrowed frame views; outputs last until the next invocation in
the same direction. Encode and Decode may run concurrently; use independent state.
Logical flags are More or Command, never LongSize. The wire encoder computes
LongSize. Multipart aggregation/routing/ownership remain outside the codec.

PLAIN also fixes its prior nonstandard wire exchange: credentials now have one-byte
lengths (at most 255 UTF-8 username bytes and 255 raw password bytes), and the client
sends INITIATE metadata after WELCOME. Applications should upgrade both local peers;
there is no compatibility mode for the old metadata-credential/READY exchange.
The authenticator receives raw password bytes, including non-UTF-8 values.

## Identity and custom sockets

PeerEnded, ZRequestContext.Peer, IPatternSink, dispatch and inbound signatures use
root-namespace ZPeer. Replace handlers accepting IZConnection with ZPeer. Its Id
is diagnostic only and a reconnect produces a new object; ROUTER ids are unchanged.
A peer identity cannot send, read or dispose a connection.

Custom socket subclasses keep the protected ZSocketBase/ZQueueSocketBase
construction with options, dispatch, type and inbound decisions. Lifecycle state
is private to the runtime; the public facade retains protected Pool, StateLock,
ThrowIfClosed, TrackBackground and SendAsyncCore. Queue-capable subclasses use
ZQueueSocketBase; configuration is resolved without overriding a virtual capability
probe. Internal library coordinators use explicitly injected send/retire/snapshot
capabilities rather than retaining socket references.

Generic synchronous teardown remains the callback-safe way to request closure.
Awaited teardown and DisposeAsync must not be awaited from a callback whose own
completion they need. Public PeerEnded runs after internal cleanup. Exceptions
from that event may fault teardown completion but cannot skip resource reclamation.
