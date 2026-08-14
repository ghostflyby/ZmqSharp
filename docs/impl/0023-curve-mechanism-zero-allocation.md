# 0023 - CURVE Mechanism Zero-Allocation

Status: accepted
Date: 2026-08-14
Revision: 2

Eliminates the per-frame byte[] allocations on the CURVE traffic path and the
per-connection allocations on the CURVE handshake path. The crypto backend
moves from allocate-and-return to destination-span signatures; the session
connection reuses per-connection buffers; the handshake uses `stackalloc`.
The mechanism seam itself (0016) stays untouched - it is already
`ReadOnlyMemory`/`Span`-shaped. The library has never shipped a release, so
the backend's breaking signature change carries no compatibility burden.

**Implemented** (this revision): `Key32` + destination-span backend +
hand-written Salsa20/Poly1305 cores (locked by the libsodium known vectors),
zero-allocation `CurveSessionConnection`, stackalloc handshake, cached local
READY body, and a measured seal/open allocation gate in `ZmqSharp.AllocationTests`
(both directions steady-state allocation-free).

## 1. Problem

The security seam (0016) is allocation-clean by design: `ZMechanismCommand`,
`LocalReadyBody`, and `WriteCommandAsync` are `ReadOnlyMemory<byte>`, and
`ZmtpCommandCodec`/`ZmtpCommands` operate on spans. The byte[] pressure is
entirely inside the CURVE package, in two places:

**Traffic path (hot, per frame).** `CurveSessionConnection.SealFrame` builds
the plaintext, nonce, keystream, ciphertext, tag+body, and wire buffers as
fresh `byte[]` per frame; the backend (`BouncyCastleCurveCrypto`) adds its own
keystream and input arrays plus a `new XSalsa20Engine` / `Poly1305` per call.
Measured against the current code, one sealed send frame allocates roughly
eight `byte[]` (~1 KB) and five small objects; the receive path is
equivalent. By comparison the clear receive path is bounded at ~216 B per
message (one pool rent, 0008 / AllocationTests) and the clear send path is
steady-state allocation-free - the CURVE path is 5-10x its plaintext
counterpart and is the only mechanism that allocates per frame.

**Handshake path (cold, per connection).** The CURVE handshake
(`CurveMechanism` client and server sessions) builds every fixed-size stage
buffer with `new byte[...]`: the 200-byte HELLO, the 128-byte WELCOME
plaintext, nonces, the vouch, the INITIATE body, the cookie, and multiple
`RandomBytes(16)` tails - roughly 25-35 small gen-0 arrays per connection.
Cold, but a connection flood pays the whole bill before
`maxIncompleteHandshakes` (0016) caps it.

The allocator has no way to avoid this today: `ICurveCryptoBackend` returns
`byte[]` from every operation, so even a caller with a reusable buffer is
forced to copy.

## 2. Design decisions

**D1 - The backend writes into caller-provided destinations; no operation
returns a `byte[]`.** Every primitive gains a `Span<byte>` destination whose
size the caller reserves up front. Output sizes are protocol-fixed (32-byte
keys, 16-byte tags, 64-byte signatures) or computable from the input, so the
caller always knows the exact size. The library has never shipped, so this is
a straight re-shape, not an additive overload.

**D2 - `CurveKeyPair(byte[], byte[])` becomes a `Key32` value type.** A
32-byte key stored as four `ulong` fields (explicit layout), with span views
for the crypto primitives. The per-connection ephemeral key pair and the
per-socket long-term key pair stop being two heap arrays each. `Key32` is
AOT-safe: no reflection, no dynamic codegen, plain value semantics.
The struct and its fields are deliberately **mutable** (the span views are
built over the storage): a readonly struct would force a defensive copy on
every field access, silently returning stale bytes whenever the write is not
inlined - a real failure mode this implementation hit and fixed.

**D3 - The session connection reuses per-connection buffers.** `SealFrame`
and the read path keep a small set of buffers rented once per connection
(a plaintext buffer, a wire buffer, a plain-frame reconstruction buffer, one
`byte[24]` nonce per direction whose tail 8 bytes are rewritten). Growth
follows the `ZMechanismContext` / `ZmtpParser` scratch pattern (double or
`ArrayPool` rent, shrink past a threshold). The steady state is zero
allocations per frame.

**D4 - The handshake uses `stackalloc`; the traffic path uses the pool.**
All `Build*` / `Parse*` stage methods are synchronous with fixed sizes ≤ 257
bytes, so their intermediate buffers go on the stack - the whole CURVE
handshake completes with zero heap allocation. Two peer-sized buffers are
the exception and are **pooled, never stackalloc'd** (review hardening):
the INITIATE plaintext and the boxed READY metadata are sized by
unauthenticated peer input bounded only by `maxCommandSize`, and a stack
allocation that large lets any TCP peer crash the server process with an
uncatchable stack overflow. The traffic path handles variable-size payloads
inside async methods (a frame's plaintext cannot live on the stack across
the next read), so it reuses rented connection buffers instead - consistent
with the existing `MemoryPool` conventions (0006 3.6).

**D5 - Unbox authentication runs before decryption.** `TryUnbox` /
`TrySecretBoxOpen` verify the Poly1305 tag with
`CryptographicOperations.FixedTimeEquals` first and only write the plaintext
to the destination after the tag matches; a failed open returns false and
leaves the destination untouched. This preserves the current constant-time
behavior (the tag check never depends on message content).

**D6 - Cold-path owned copies stay owned copies.** `PeerReadyBody` is
retained as an owned copy (the mechanism context's scratch is reused by the
next read); it changes type to `ReadOnlyMemory<byte>` for a cleaner struct but
does not take pool ownership - a ~26-byte, once-per-connection value does not
justify an `IMemoryOwner`/`IDisposable` on a public struct. Likewise the
local READY body is built once per socket instead of once per connection.

## 3. The new backend surface

```csharp
/// <summary>A 32-byte key as a value type; explicit layout, AOT-safe.</summary>
public readonly struct Key32
{
    private readonly ulong a, b, c, d;
    public ReadOnlySpan<byte> Span { get; }
    public void CopyTo(Span<byte> destination);
    public static Key32 From(ReadOnlySpan<byte> source);
    public bool Equals(Key32 other);
    public override bool Equals(object? obj);
    public override int GetHashCode();
    public static bool operator ==(Key32 left, Key32 right);
    public static bool operator !=(Key32 left, Key32 right);
}

public interface ICurveCryptoBackend
{
    void GenerateKeyPair(out Key32 publicKey, out Key32 secretKey);

    void DeriveSharedSecret(ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination);                       // 32 bytes

    int Box(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> senderSecret, ReadOnlySpan<byte> recipientPublic,
        Span<byte> destination);                       // tag(16) + ciphertext

    bool TryUnbox(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> recipientSecret, ReadOnlySpan<byte> senderPublic,
        Span<byte> destination, out int written);      // false on auth failure

    int SecretBox(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination);

    bool TrySecretBoxOpen(ReadOnlySpan<byte> boxed, ReadOnlySpan<byte> nonce,
        ReadOnlySpan<byte> key, Span<byte> destination, out int written);

    void Sign(ReadOnlySpan<byte> message, ReadOnlySpan<byte> secretKey,
        Span<byte> signature);                         // 64 bytes

    bool Verify(ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature,
        ReadOnlySpan<byte> publicKey);

    void RandomBytes(Span<byte> destination);
}
```

`CurveMechanism` configuration follows the value type:

```csharp
// Client: long-term key pair plus the server's public key.
public CurveMechanism(ICurveCryptoBackend crypto, Key32 clientLongTermKey, Key32 serverPublicKey);

// Server: the long-term key pair clients authenticate against.
public CurveMechanism(ICurveCryptoBackend crypto, Key32 serverLongTermKey);
```

## 4. Per-frame allocation ledger (traffic path)

Send (`SealFrame`), current code on the left, target on the right:

| Allocation | Today | Target |
|---|---|---|
| plaintext `[1+payload]` | `new` | connection buffer |
| nonce `[24]` | `new` | stack, tail rewritten |
| keystream + zero input | `new` x2 | stack (hand-written cores) |
| ciphertext | `new` | destination region of wire buffer |
| tag+body `[16+ct]` | `new` | destination region of wire buffer |
| wire frame | `new` | connection buffer |
| engine objects | `new` x2 per call | none (hand-written Salsa20/Poly1305 cores) |

Receive (`ReadFrameAsync`), likewise: three read buffers and the frame
reconstruction buffer move into connection scratch; the nonce becomes a
fixed field; the backend writes the plaintext straight into the
reconstruction buffer. A per-frame count of zero new byte[] is the target.

Implementation deltas: the shared seal buffer is written under a small
per-connection `SemaphoreSlim` send gate (sealing + the raw write); the gate
is held for a **whole message** in `SendAsync`, preserving message-level
atomicity under concurrent sends (0021). The read staging array is retained
across frames (length-tracked, never nulled), so the next frame reuses it.
The client derives its long-term public key from the configured secret at
session creation, since the wire carries the public while the vouch seals
with the secret. Review hardening (this revision): frame reconstruction
recombines the ZMTP LongSize bit for payloads > 255 bytes; the boxed
Command flag uses the real `0x04` value (never `0x02`, which would collide
with LongSize); the peer-sized handshake buffers are pooled (D4); and the
session's `Dispose` leaves the send gate to the raw connection teardown so
an in-flight send can always release it.

Cold-path ownership: the handshake still allocates the reconstructed HELLO /
INITIATE command bodies, the cookie, the opened peer metadata (the owned
`PeerReadyBody` copy), and the metadata dictionary - all once per
connection, all required by the seam's borrowed-scratch lifetime rule (0016).

## 5. Backend internals

- `GenerateKeystream` drops the `new byte[length]` input array; the XSalsa20
  keystream is generated block-by-block into the destination region.
- The `crypto_box` composition keeps its current shape (XSalsa20 keystream,
  first 32 bytes as the one-time Poly1305 key, then XOR the message). The
  Salsa20/HSalsa20 core and Poly1305 are hand-written in the style of the
  existing `Hsalsa20`, so the primitives are stateless and allocate nothing;
  BouncyCastle is used only for X25519 and Ed25519. The hand-written cores
  are locked byte-for-byte against libsodium by the known-vector tests.

## 6. Cold-path deltas

- `ZMechanismResult.PeerReadyBody` changes from `byte[]` to
  `ReadOnlyMemory<byte>`; the copy itself is required and unchanged (D6).
- The local READY body (`ZmtpCommands.BuildReady(type.Name)`) is built once at
  socket construction and reused for every connection.

## 7. Testing

- **Known vectors stay byte-identical.** `LibsodiumKnownVectorTests` keeps
  every vector value; only the backend call shapes moved to destination spans
  (0023). These vectors lock the hand-written Salsa20/HSalsa20/Poly1305
  cores byte-for-byte against libsodium.
- **Allocation gates for the CURVE traffic path.** `CurveTrafficAllocationTests`
  in the allocation project drives seal and open over an in-process fake
  connection and asserts both loops stay under 1 KB across 1000 frames -
  steady-state allocation-free (the clear path has the same gate, 0008).
- **Session-level traffic coverage.** `CurveSessionTrafficTests` locks the
  frame reconstruction and the send gate: a > 255-byte payload round-trips
  with the LongSize flag recombined, a multi-frame message arrives whole with
  the More flags, concurrent sends never interleave frames, and tampered
  ciphertext or a replayed nonce is rejected with a protocol error.
- **End-to-end interop is unchanged.** `CurveEndToEndTests` (ZmqSharp-to-
  ZmqSharp in both directions plus the wrong-server-key fault) pass with the
  new backend shapes and the same wire bytes.
- **AOT analyzers.** `Key32` and the new signatures introduce no reflection;
  `IsAotCompatible` stays clean in both projects.

## 8. Milestones

| # | Work item | Size | Status |
|---|-----------|------|--------|
| 1 | `Key32` value type + `ICurveCryptoBackend` destination signatures + `BouncyCastleCurveCrypto` rewrite | Medium | **Implemented** |
| 2 | `CurveSessionConnection` buffer reuse (send and receive) | Medium | **Implemented** |
| 3 | `CurveMechanism` handshake `stackalloc` + `CurveKeyPair` removal | Small | **Implemented** |
| 4 | Cold-path deltas: `PeerReadyBody` type, cached READY body | Small | **Implemented** |
| 5 | CURVE allocation gate + backend call-shape migration in tests | Small | **Implemented** |

Milestones 1 and 2 land together - the destination signatures are what make
the buffer reuse possible; 3-5 follow independently.

## 9. Rejected alternatives

- **Allocate-and-return with a pool**: the backend returns an
  `IMemoryOwner<byte>`. Rejected: pushes pool ownership through a public
  interface and still allocates the owner wrapper per call; destination spans
  are simpler and let the caller own the buffer (D1).
- **`stackalloc` on the traffic path.** Rejected: payload sizes are variable
  and the path is async; stack memory cannot outlive an await. Pooled
  connection buffers cover it (D4).
- **Pool-owned `PeerReadyBody`.** Rejected: a ~26-byte, once-per-connection
  value does not justify ownership semantics on a public struct (D6).
- **Compatibility overloads on the backend.** Rejected outright: the library
  has never shipped, so the old shape is deleted, not deprecated.

## 10. Related documents

- 0016 - security mechanism boundary (the seam this keeps untouched).
- 0017 - CURVE mechanism evaluation (backend shape it replaces; wire-format
  lock and libzmq interop are the constraints).
- 0008 / AllocationTests - the allocation gate conventions the new CURVE
  gate follows.
