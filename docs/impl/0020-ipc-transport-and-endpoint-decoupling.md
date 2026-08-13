# 0020 - ipc Transport and Endpoint Decoupling

Status: accepted
Date: 2026-08-13
Revision: 1

Implements work item #2 of 0015 (ipc + parameterized tests): the socket
system no longer binds to TCP, and `ipc://` (Unix domain sockets) closes the
one gap in the libzmq transport set. It completes the endpoint decoupling that
0015 section 5 described as "nearly free because `IZTransport` is
endpoint-agnostic."

## 1. Problem

`SocketTransport` hard-coded TCP in two places, so no other endpoint family
could flow through it:

- `new Socket(SocketType.Stream, ProtocolType.Tcp)` fixed the address family
  to InterNetwork regardless of the endpoint;
- `NoDelay = true` was applied unconditionally, but `NoDelay` is TCP-only and
  throws on a Unix domain socket.

On top of that, the string-endpoint facade (`ZSocketExtensions`) understood
only the `tcp://` scheme and always resolved to `IPEndPoint`.

## 2. Endpoint-driven socket construction

`SocketTransport` now constructs the socket from the endpoint:

```csharp
new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified)
```

Both `IPEndPoint` (InterNetwork) and `UnixDomainSocketEndPoint` (Unix) produce
a stream socket, so one transport serves both endpoint kinds with no new
transport type (0015 section 5.1). TCP-only tuning (`NoDelay`) is applied only
when the endpoint's address family is not `AddressFamily.Unix`. Accepted
sockets inherit the listener's family; `StartAsync` derives the family from
the accepted socket's endpoint and applies the same rule.

## 3. Endpoint parsing

`ZSocketExtensions.ParseEndpointAsync` now returns `EndPoint` and understands
two schemes:

- `tcp://host:port` - unchanged behavior (DNS resolution, literal addresses).
- `ipc://path` - a `UnixDomainSocketEndPoint`. An absolute path keeps its
  leading slash (`ipc:///tmp/foo` -> `/tmp/foo`); a relative path
  (`ipc://my.sock`) resolves against `Path.GetTempPath()`, mirroring libzmq's
  default IPC directory. The URI parser splits the two forms: an absolute
  form lands in `AbsolutePath` (query excluded), while a relative form places
  the path in `Host` with `AbsolutePath` at `/` - the parser falls back to
  `Host` in that case. Verified against the real URI parser.

Windows absolute paths round-trip through the URI parser as well: the parser
normalizes backslashes to forward slashes and drops the authority, so
`ipc://C:\Users\app\foo.sock` parses to `C:/Users/app/foo.sock`, and
`Path.Combine` keeps it unchanged (on Windows the drive-qualified path is
fully qualified). URI parsing itself is platform-independent.

Unknown schemes keep throwing `NotSupportedException`. The generic transport
dispatch (`ConnectAsync<EndPoint, SocketTransport>`) already existed and is
unchanged.

## 4. Bind lifecycle

A Unix domain `Bind` creates a filesystem entry for the path. libzmq semantics
treat a stale entry as reusable: `SocketTransport` records the bound path and
`File.Delete`s it on dispose, so a later bind of the same path succeeds
instead of failing with EADDRINUSE. TCP binds record no path and do nothing.

## 5. Transport parameterized suites

The existing real-socket suites run once per transport (0015 section 5.4):

- `TestTransports.TransportKinds()` provides `TheoryData<TransportKind>` with
  both `Tcp` and `Ipc` on every platform: the ipc cases run on Windows CI
  too, because ZmqSharp's ipc is real AF_UNIX there (BCL support since
  Windows 10 1803).
- `TestTransports.GetEndpoint(kind)` builds a fresh endpoint per invocation:
  a free TCP port, or a unique temp-path socket file.
- Parameterized files: `ZSocketTests` (receive policy, limits, queue drop
  modes, peer churn, concurrent sends), `ZReqRepTests`, `ZPlainMechanismTests`
  (PLAIN end-to-end), `CustomSocketTypeTests` (custom-type pair and rejection),
  `InboundPolicyTests` (transform/consume delivery), `DispatchPolicyTests`
  (multi-select broadcast).

Tests that probe the transport or protocol boundary rather than the endpoint
layer stay TCP-only: raw-`TcpClient`/`TcpListener` wire/handshake cases,
fake-transport allocation measurements, and the NetMQ interop suite.

## 6. ipc-specific coverage

`ZSocketIpcTests` covers the surface that does not exist for TCP:

- the bind path is unlinked on socket dispose;
- the same path can be bound again after dispose;
- connecting to a missing path fails cleanly (socket/IO error);
- PUSH fans out across multiple PULL peers over ipc.

## 7. Interop: NetMQ ipc is not AF_UNIX

0015 section 5.4 assumed the NetMQ interop suite could run over `ipc://` on
Unix. Investigation during implementation shows that assumption is wrong for
the pinned NetMQ version: `NetMQ.Core.Transports.Ipc.IpcAddress` resolves an
ipc address by hashing the name to a loopback TCP port
(`IPEndPoint(IPAddress.Loopback, stableHash % 55536 + 10000)`), so NetMQ's
`ipc://` is a TCP transport that never touches a Unix domain socket. An
in-library smoke test confirmed it binds no filesystem entry and connects over
TCP. Consequently a NetMQ `ipc://` endpoint can never connect to a real
AF_UNIX endpoint, and the proposed `IpcInteropTests` were removed. ipc remains
validated by the transport-parameterized in-library suites plus the
ipc-specific lifecycle tests; ZMQ interoperability with real libzmq Unix
domain sockets is a separate evaluation (section 8).

## 8. Differentiation

libzmq supports ipc as true Unix domain sockets on both Unix and modern
Windows (its IPC transport uses `afunix.h` and `open_socket(AF_UNIX, ...)` on
Windows); NetMQ's ipc is a TCP hash, not a Unix domain socket on any platform.
ZmqSharp implements ipc as real AF_UNIX sockets (the ZMTP sense of the term),
on every platform - on Windows 10 1803+ AF_UNIX exists in the BCL. The
differentiator vs. NetMQ is noted in the README.

## 9. Acceptance

- The transport suites run green over both tcp and ipc on all three CI
  platforms (ubuntu, windows, macos).
- `dotnet format --verify-no-changes` stays clean.
- 0015 work item #2 is complete, with the section 5.4 interop premise
  corrected as documented in section 7.
