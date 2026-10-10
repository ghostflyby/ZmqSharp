# 0028 - Architecture and API Review Roadmap

Status: draft
Date: 2026-10-07

Records the follow-up review of the architecture/API assessment and the agreed
implementation sequence. Stage one is specified in 0029. Stages two and three
are specified by 0030 and implemented after the correctness work, in the
user-authorized role, frame-boundary and registry change sequence. 0030 is
accepted.

## 1. Review conclusions

| Finding                               | Assessment and disposition                                                                                                                                                                                                        |
|---------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A1: mixed connection responsibilities | Confirmed: transport, wire send, and receive callbacks share one interface. This is type coupling within one assembly, not an assembly dependency cycle. Split responsibilities while preserving ownership and message atomicity. |
| A2: duplicate peer bookkeeping        | Confirmed: core and queue surface own separate records/snapshots. Their consumers differ; unify lifecycle and records rather than simply deleting a snapshot.                                                                     |
| A3: callback forms and dead seam      | `ZFrameHandlerAsync` is used internally; sync borrowed and async delivery lifetimes differ. Retire `IZMessageSink` and the unused end-handler registration after parser receives its callback explicitly.                         |
| A4: lifecycle base                    | Fold `ZAsyncState` into `ZSocketBase` while retaining protected lifecycle capabilities.                                                                                                                                           |
| A5: inbound coordination              | REQ, REP, and XPUB are stateful coordinators with outbound capabilities, not three fully independent strategies. Describe composition honestly; avoid a broad replacement interface.                                              |
| A6: test seams                        | Existing fake-transport tests are useful, but direct state-machine tests are needed for request coordination, reader wakeups, and receive materialization.                                                                        |
| B1: Task/ValueTask                    | Keep `Task<ZMessage>` for requests that wait for a reply and `ValueTask` for sends. Return type uniformity alone is not a reason to change them.                                                                                  |
| B2/B3: endpoints                      | Add cancellation, string teardown, and a clear distinction between initiating shutdown and awaiting cleanup.                                                                                                                      |
| B4: subscription input                | Array parameters elsewhere intentionally transfer ownership. Subscriptions must instead copy caller data, expose a span input, and match removals by content.                                                                     |
| B5: receive rejection                 | Make the structured payload and exception public in the root namespace and align 0018.                                                                                                                                            |
| B6: domain types in root signatures   | Cross-domain references are not inherently invalid. Ordinary peer identity should nevertheless expose neither raw I/O nor disposal.                                                                                               |
| B7: naming                            | Defer bulk renaming until behavioral and architectural boundaries are stable.                                                                                                                                                     |
| B8: CURVE backend                     | Move public-key derivation behind the backend. Keep current package dependency and inline key storage; package isolation and key text formats are separate work.                                                                  |
| B9: construction/default paths        | Retain message factories from 0026: they return different message forms. Configuration and mechanism defaults serve different roles.                                                                                              |
| B10: empty transport options          | Remove the placeholder when changing the transport interface in stage two.                                                                                                                                                        |

Source inspection also identified request tasks that can remain pending after
send cancellation or malformed reply parsing. These are stage-one correctness
issues, ahead of naming cleanup. Tests must establish the actual failure and
completion paths rather than infer correctness from signatures.

## 2. Stage one: correctness and public behavior

Implement 0029: endpoint shutdown completion, per-request outcomes and buffer
lifetimes, copied subscription prefixes, public rejection diagnostics, lifecycle
base removal, and backend public-key derivation. Migrate repository callers,
README, and the numbered documents together. Breaking API changes are permitted;
no obsolete compatibility layer is required.

## 3. Stage two: transport, session, and peer boundaries

- Transports provide asynchronous byte I/O and connection abort. Preserve
  sequence writes and scatter/gather optimization; remove empty transport options.
- ZMTP sessions provide frame, command, and message writes. Serialization spans
  the whole multipart message, not each byte segment.
- Parser accepts a read capability and `ZFrameHandlerAsync` explicitly. Remove
  receive registration from connections and retire `IZMessageSink`.
- Security returns an optional frame codec and peer metadata. CURVE remains a frame-based
  authenticated transformation, not an ordinary encrypted byte stream.
- Introduce a stable root-namespace peer identity without send/read/dispose
  methods for events, request contexts, and selection policies. Internal records
  resolve identity to actual connections.
- Review whether to retire `ZCurrentPeerDispatch` during the public policy
  migration. Stage one leaves it in place, but built-in REQ now uses injected
  capabilities, only tests still reference it, and its target-setting methods
  are internal. Retention would require an explicit public customization use case.
- Separate handshake orchestration from the wire codec. Document the sole owner
  of raw connection, session, parser, codec and their pooled buffers.

Gate this stage on NULL/PLAIN/CURVE wire interoperability, multipart atomicity,
exactly-once disposal, and existing steady-state allocation budgets. Do not move
public domain types merely to make namespace dependency arrows look acyclic.

## 4. Stage three: unified peer lifecycle and receive state

- Core owns a registry and published peer records including endpoint identity,
  handshake gate, session, cancellation, completion, aggregation, and optional
  receive queue state. Queue delivery no longer owns a second peer dictionary.
- Internal lifecycle cleanup runs independently of public event subscriber order.
  Preserve closure during handshake, send gating, retirement from routing, and
  exactly-once reclamation of messages without consumers.
- Extract aggregate reader, wake gate, and materializer into internal components
  with direct unit-test seams. Parse receive mode and validate configuration once.
- Pass coordinator instances/capabilities explicitly for REQ/REP/XPUB instead of
  recovering them by casting `InboundPolicy` or attaching socket references later.

Bulk naming changes, key-format convenience APIs, and a separate crypto backend
package are outside these stages. Keep public namespaces organized by consumer
domain and internal namespaces aligned with implementation directories.

## 5. Replacement-model implementation (0030)

The role, byte/frame and single-registry stages are implemented. Peer identity is
ZPeer; transports expose only byte I/O/Abort; CURVE returns CurveFrameCodec.
Coordinator Attach/casts and queue peer dictionaries are removed. Readers, wake
signals and materialization have direct internal tests. Request and endpoint
correctness guarantees from 0029 remain covered by their regression suites.
The existing ZCurrentPeerDispatch extension is retained; no additional bulk policy
retirement or naming migration is needed for the built-in coordinator model.

Reference verification also found that the old PLAIN wire sequence used metadata
credentials and READY instead of INITIATE. The implementation now uses RFC 24
length-prefixed credentials and HELLO/WELCOME/INITIATE/READY. See 0030 for
allocation measurements and historical build/test/AOT and libzmq interop evidence.
