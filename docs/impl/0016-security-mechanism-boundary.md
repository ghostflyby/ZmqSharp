# 0016 - Replaceable Security Mechanism Boundary

Status: draft
Date: 2026-08-12
Revision: 2 (2026-10-08)

Mechanisms are configured once per socket, matched by their greeting name, and
create a per-connection handshake state machine. Revision 2 aligns the public
contract and ownership model with 0030. The original connection-wrapper and
connection-direction role assumptions are retired.

## 1. Why the original boundary was attractive

Separating greeting/handshake from the traffic parser removed hard-coded NULL
logic and allowed PLAIN to drive HELLO/WELCOME/READY. Returning the unchanged raw
connection for clear mechanisms kept call sites simple. Allowing a replacement
connection appeared to make future encryption pluggable without changing parsing.

That model conflated TLS byte encryption with CURVE authenticated frame encryption.
CURVE required a wrapper to recreate a plaintext byte-frame stream solely for the
parser to decode it again. Selecting client/server from connect/accept also confused
network origin with configured security role. See 0030 section 1 for the resulting
replacement model and standards references.

## 2. Decisions

- One mechanism per socket, selected by configuration and matched by Name.
  No discovery, reflection or runtime registration from wire strings.
- Role is fixed by the mechanism configuration. NULL has None; PLAIN/CURVE
  constructors select Client or Server. Bind/connect do not change it.
- A mechanism owns its command sequence, including the ordering of READY.
  Socket runtime owns the local Socket-Type/identity metadata and validates the
  peer type after the mechanism completes.
- Handshake is the exclusive byte writer until the establishment gate succeeds.
  The established session then serializes complete multipart messages.
- Authentication may also establish frame encryption. A mechanism returns an
  optional IZFrameCodec, not a byte connection wrapper. TLS can independently
  remain a transport property; ZMTP mechanisms themselves do not stack.

## 3. Public extension contract

These types live in `ZmqSharp.Security`; frame transformation types live in
`ZmqSharp.Zmtp`. Their directory organization is specified in 0018.

```csharp
public enum ZMechanismRole { None, Client, Server }

public interface IZSecurityMechanism
{
    string Name { get; }
    ZMechanismRole Role { get; }
    IZMechanismSession CreateSession();
}

public interface IZMechanismSession
{
    ValueTask<ZMechanismResult?> RunAsync(
        ZMechanismContext context, CancellationToken token);
}
```

The context exposes `LocalReadyBody`, `MaxCommandSize`, `WriteCommandAsync` and
`ReadCommandAsync`. Returned command name/argument memories are borrowed until the
next context read; copy data that must survive. There is no raw Connection property.
The runtime constructs and disposes the context around the actual handshake task.

```csharp
public readonly struct ZMechanismResult
{
    public ZMechanismResult(IZFrameCodec? codec, ReadOnlyMemory<byte> peerReadyBody);
    public IZFrameCodec? Codec { get; }
    public ReadOnlyMemory<byte> PeerReadyBody { get; }
}
```

PeerReadyBody must remain valid after the context is disposed. Successful codec
ownership transfers to the session. A failed mechanism must clean any resources
it allocated before returning. The runtime owns raw byte connections; a mechanism
must not dispose them. See 0030 for codec borrowing and directional concurrency.

## 4. Runtime orchestration

`ZmtpHandshake` is internal to `ZmqSharp.Sockets`. It emits the configured greeting,
reads/validates the peer greeting, compares mechanism names and runs CreateSession.
Mismatch sends ERROR and faults establishment. NULL always emits as-server = 0;
a configured server emits 1. Peer role-bit differences are tolerated, while local
role remains configuration-driven.

After a result, the runtime adopts the codec, validates peer Socket-Type,
registers routing metadata, constructs the parser and publishes establishment.
The parser receives a byte reader, explicit frame callback and optional codec.
No transport callback registration is involved.

Timeout and cancellation abort underlying I/O, await the actual handshake task,
and only then release its context. Accepted-handshake concurrency limits use the
connection origin, never mechanism.Role. Sending before establishment waits on
the peer gate; stopping cancels that gate.

## 5. Built-in mechanisms and package boundary

NULL immediately sends READY and reads peer READY, returning no codec. PLAIN
runs HELLO/WELCOME/INITIATE/READY, authenticates on the configured server and likewise
returns no codec. Credentials/authenticator select PLAIN role; a client may bind
and a server may connect.

CURVE lives in the optional `ZmqSharp.Security.Curve` assembly. Its configured
client/server runs HELLO/WELCOME/INITIATE/READY and returns CurveFrameCodec.
The core package has no dependency on a crypto backend. Public-key derivation and
all cryptographic operations go through ICurveCryptoBackend. The default backend
still depends on BouncyCastle; backend package isolation is separate work.

The public seam is usable by external assemblies with no InternalsVisibleTo.
Direct tests implement a mechanism and codec using only these public contracts.
Native AOT validation exercises the same extension contract.

## 6. Validation

Greeting tests enforce NULL's zero bit in both directions. PLAIN/CURVE exchange
suites cover normal and reversed topologies; a pyzmq/libzmq PLAIN harness
checks all four role/topology combinations; CURVE tests cover TCP and real IPC.
NetMQ CURVE interop covers both local roles with both bind and connect. Component
lifecycle tests verify that cancellation cannot dispose a still-running handshake
context, and that codec disposal precedes public termination notification.

Receive authentication/replay/limits, multipart serialization and allocation
acceptance belong to 0030. Behavioral endpoint and request guarantees from 0029
remain in force.
