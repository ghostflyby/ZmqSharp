# 0018 - Namespace and Directory Organization

Status: draft
Date: 2026-08-12

Reorganizes the namespace and directory layout so that the public API is
organized around a single top-level namespace for basic usage, sub-namespaces
for domain-specific feature areas, and internal namespaces that mirror the
directory structure. Supersedes the namespace statements of 0001 section 12 and
AGENTS.md and resolves the namespace open question of 0016 section 13.

## 1. Problem

The previous layout mirrored directories one-to-one and had no top-level
`ZmqSharp` namespace:

- `ZmqSharp.Messages` - message model
- `ZmqSharp.Sockets` - sockets and patterns (the docs prescribed
  `ZmqSharp.Patterns`; the code used `ZmqSharp.Sockets`)
- `ZmqSharp.Transports` - transport seam
- `ZmqSharp.Zmtp` - ZMTP wire codec plus the security mechanism surface mixed
  together (0016 section 13 flagged the split question)

A consumer writing a basic program (README's examples) must import
`ZmqSharp.Sockets` for `ZSocket` and `ZmqSharp.Messages` for `ZMessage` -
`using ZmqSharp.Sockets;` alone did not compile the REQ/REP example. The
namespace surface was organized by internal layer, not by what a consumer
needs.

## 2. Principles

1. **Directories reflect development areas** - where a developer works on a
   feature, not how the API is consumed.
2. **Namespaces reflect the public API organization**, not the directory
   layout - a single `using ZmqSharp;` provides the basic necessary types and
   functionality.
3. **Sub-namespaces are domain-specific feature areas** for specific needs.
4. **Internal namespaces generally match the directory layout** (internal API
   is an implementation concern, so the layer-by-layer layout is fine there).

## 3. Target layout

### Top-level `ZmqSharp` - base public API

A single `using ZmqSharp;` covers all basic usage:

- Entry points and surfaces: `ZQueueSocket<TSocket>`, the concrete socket
  composition roots (directly constructed, 0022), `IZSocket`, `IZCallbackSocket`,
  `IPatternSink`.
- Base classes: `ZSocketBase`, `ZAsyncState` (public because public socket
  types derive from it; its members are protected).
- Concrete sockets: `ZPairSocket`, `ZDealerSocket`, `ZReqSocket`, `ZRepSocket`,
  `ZPushSocket`, `ZPullSocket`, `ZRouterSocket`, `ZPubSocket`, `ZSubSocket`,
  `ZXPubSocket`, `ZXSubSocket`.
- REP request value: `ZRequestContext` (the handler context of the README
  REQ/REP example).
- Message model: `ZMessage`, `ZFrame`, `ZSegment`, `ZSegments`,
  `ZSingleMessage`, `ZMultiMessage` (with their nested enumerators).
- Configuration: `ZSocketOptions`, `ZQueueSocketOptions`, `ZSecurityOptions`,
  `ZSocketExtensions`.
- Receive/queue tuning: `ZReceiveMode`, `ZReceiveAllocation`, `ZReceiveContext`,
  `IZReceivePolicy`, `ZDecide`, `ZDelegateReceivePolicy`, `ZReceiveOptions`,
  `ZReceiveRejectionReason`, `ZReceiveRejection`, `IZQueueFactory`,
  `ZQueueFactory`. These live with the configuration they feed
  (`ZQueueSocketOptions`), so one import covers all configuration scenarios.
- Exceptions: `ZeroMqProtocolException`.

### Sub-namespaces - domain-specific feature areas

- `ZmqSharp.Transports` (unchanged): `IZTransport`, `IZTransport<TSelf,TE>`,
  `IZConnection`, `SocketTransport`, `ZTransportOptions`.
- `ZmqSharp.Zmtp` (wire codec only): `ZmtpParser`, `ZmtpFrameEncoder`,
  `ZmtpCommands`, `ZmtpCommandCodec`, `IZMessageSink`, `ZFrameHandler`,
  `ZFrameHandlerAsync`, `ZmtpFrameFlags`. `ZmtpFrameFlags` is public because
  the CURVE example assembly (a separate project without
  `InternalsVisibleTo`) reproduces the frame flag bits.
- `ZmqSharp.Security` (new): `IZSecurityMechanism`, `IZMechanismSession`,
  `ZMechanismContext`, `ZMechanismResult`, `ZMechanismCommand`,
  `ZMechanismRole`, `ZNullMechanism`, `ZPlainMechanism`,
  `ZPlainAuthenticator`, `ZMechanismException`. Mechanism authors are a
  distinct audience from wire-codec consumers; the CURVE example mechanism
  (ZmqSharp.Security.Curve) already lives under this area. Note that a
  mechanism author also imports `ZmqSharp.Zmtp` for the shared wire rules
  (`ZmtpCommandCodec`, `ZmtpCommands`) - the codec is de-facto mechanism
  infrastructure.
- `ZmqSharp.Patterns` (new): the socket-composition seams (0015 section 2.1 /
  0019) - `IZDispatchPolicy` and the `Z*Dispatch` policies,
  `IZInboundPolicy` with `ZInboundAction` / `ZInboundDecision` /
  `ZInboundDecide` / `ZDelegateInboundPolicy` / `ZInboundPolicy`, and the
  socket identity `ZSocketType` / `ZSocketTypes` (`ForCustom`). These are the
  extension face for custom socket types: a developer subclassing
  `ZSocketBase` (in `ZmqSharp`) imports `ZmqSharp.Patterns` for the
  constructor's seam arguments, mirroring how a mechanism author imports
  `ZmqSharp.Security`. Users constructing the built-in sockets never write
  these types and do not pay the import.

### Internal API - namespaces match the directory layout

- `ZmqSharp.Sockets` (internal): the pattern-assembly helpers
  `ZDelimiterFraming` (REQ/REP delimiter wire format), `ZTopicFilter` /
  `ZTopicFilterPolicy` (SUB subscriptions), and the REQ/REP consume cores
  `ZReqCore` / `ZRepCore`.
- `ZmqSharp.Zmtp` (internal): `ZmtpHandshake`, `ZmtpGreeting`.
- `ZmqSharp.Transports` (internal): `ZConnection`.
- Internal helpers mixed into files with public types (`ZSequence`,
  `ZBoundedQueueFactory`, `ZReceiveGuard`, `ZFrameAllocator`, ...) share the
  top-level namespace with those types - "generally match" the directory.

## 4. Directory layout

```
ZmqSharp/
├── Messages/       (ZMessage, ZFrame, ZSegment, ...)
├── Patterns/       (dispatch/inbound/type seams for custom sockets)
├── Sockets/        (public socket types, options, pattern assembly)
├── Transports/     (IZTransport, SocketTransport, ZConnection)
├── Zmtp/           (ZmtpParser, ZmtpFrameEncoder, handshake internals)
└── Security/       (mechanism seam: IZSecurityMechanism, PLAIN, NULL)
```

The `Security/` directory was created by moving the ten mechanism files out of
`Zmtp/`. No other files moved; public types changed namespace declarations in
place.

## 5. Decisions

- **Security mechanisms split into `ZmqSharp.Security`** (0016 section 13
  open question, now resolved): mechanisms are a replaceable extension seam
  with their own audience; `ZmqSharp.Zmtp` keeps only the wire codec.
- **The socket-composition seams split into `ZmqSharp.Patterns`** (0015
  section 2.1 / 0019 open question, now resolved): dispatch, inbound, and
  socket-type seams are the extension face for custom socket types, a
  distinct audience from those who only construct the built-in sockets; the
  extension author pays the extra import, exactly as a mechanism author
  imports `ZmqSharp.Security`.
- **Receive/queue tuning types stay in the top-level `ZmqSharp`**: they are
  configuration for `ZQueueSocketOptions` and are needed by the same import.
- **No behavior changes**: this is a pure structural reorganization; socket
  type names, members, and semantics are unchanged.

## 6. Out of scope

- The remaining 0015 evolution work items (`ipc://`, write-path cluster) may
  choose their own namespaces when they land. The dispatch/type/inbound
  split is resolved (section 5): the seams landed in `ZmqSharp.Patterns`.
- No docs/ reorganization (0006 section 7.1 remains separate).
