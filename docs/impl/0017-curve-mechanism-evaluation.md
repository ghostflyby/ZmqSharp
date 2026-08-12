# 0017 - CURVE Mechanism Implementation Evaluation

Status: draft
Date: 2026-08-12
Revision: 1

Evaluates how to implement the ZMTP CURVE security mechanism (RFC 24 /
CurveZMQ) on top of the mechanism boundary of 0016. Two questions, answered
in order:

1. Can a CURVE mechanism be a *user-provided* implementation that plugs into
   the existing public seam, letting the user choose the crypto library?
2. What does the C# X25519 / XSalsa20-Poly1305 ecosystem look like, and which
   options are viable under the project's NativeAOT stance?

It extends 0016 (mechanism boundary, PLAIN) and supersedes 0016's CURVE
milestone row with a concrete decision path.

## 1. Conclusion up front

1. **Yes - the existing public seam fully supports a user-pluggable CURVE
   mechanism.** The mechanism writes its encrypted handshake commands through
   `ZMechanismContext` (raw bytes - the session seals them itself), and
   returns a *session connection* that encrypts on write / decrypts on read at
   the frame level through `ZMechanismResult.SessionConnection`. Both seams are
   public and were proven usable by the PLAIN mechanism (0016 milestone 2),
   which is itself public-API-only. The Crypto backend is an interface the
   user implements with their library of choice; the CURVE *protocol* (message
   sequence, nonce prefixes, key derivation, vouch) is provided as example
   code.
2. **The only actively maintained pure-managed library covering all four
   CURVE primitives is BouncyCastle.Cryptography** (X25519, Ed25519,
   XSalsa20, Poly1305; `IsAotCompatible=true`; clean NativeAOT publishing).
   The .NET BCL gets X25519 only in .NET 11 and still has no Ed25519. Native
   libsodium bindings work under NativeAOT via `DllImport`/`LibraryImport` and
   the RID-based `libsodium` NuGet package, at the cost of a native
   dependency. Chaos.NaCl and NSec are not viable (unmaintained /
   XSalsa20-Poly1305 not public).

Recommended shape: **CURVE ships as an example mechanism, not as a library
built-in.** The library provides the protocol skeleton and a crypto-backend
interface; the user supplies the backend (BouncyCastle for pure managed /
AOT, or a thin libsodium `LibraryImport` shim when a native dependency is
acceptable).

## 2. The public seam supports user-pluggable CURVE

0016 established that a mechanism is `IZSecurityMechanism` +
`IZMechanismSession.RunAsync(ZMechanismContext) -> ZMechanismResult`. Two
properties carry CURVE:

- `ZMechanismContext.WriteCommandAsync` / `ReadCommandAsync` operate on raw
  bytes. A CURVE session seals each handshake command with the current stage
  key before writing, and opens the peer's commands after reading - the
  driver never sees the ciphertext. Handshake commands in CURVE are
  individually encrypted (HELLO under the server public key, WELCOME under
  the client ephemeral key, INITIATE/READY under the session key).
- `ZMechanismResult.SessionConnection` accepts any `IZConnection`. CURVE
  returns a wrapper that decrypts on read / encrypts on write at the *frame*
  level: the ZMTP frame header stays clear, the frame body is sealed. The
  parser and the socket layer are unchanged - they already run against the
  session connection (0016 section 9).

### 2.1 The shape of the user-supplied crypto backend

The only part a user must choose a library for is this small interface; the
protocol skeleton composes it:

```csharp
// User implements this with their library of choice (BouncyCastle, a
// libsodium LibraryImport shim, ...). AOT-friendly: explicit, no reflection.
public interface ICurveCryptoBackend
{
    // X25519 key pairs and shared-secret derivation (crypto_box beforenm).
    CurveKeyPair GenerateKeyPair();
    ReadOnlySpan<byte> DeriveSharedSecret(CurveKeyPair ephemeral, byte[] peerPublic); // 32 bytes

    // crypto_box / crypto_box_open (Curve25519XSalsa20Poly1305, 24-byte nonce, 16-byte tag).
    byte[] Seal(ReadOnlySpan<byte> plaintext, byte[] nonce, byte[] boxKey);
    byte[]? Open(ReadOnlySpan<byte> ciphertext, byte[] nonce, byte[] boxKey); // null on auth failure

    // Ed25519 signature for the vouch.
    byte[] Sign(ReadOnlySpan<byte> message, byte[] signingKey); // 64 bytes
    bool Verify(ReadOnlySpan<byte> message, byte[] signature, byte[] publicKey);

    // CSPRNG for ephemeral keys and nonce tails.
    byte[] RandomBytes(int count);
}

public sealed record CurveKeyPair(byte[] PublicKey, byte[] SecretKey);
```

The `boxKey` for each stage is derived by the protocol skeleton (section 2.3)
from the two X25519 shared secrets, exactly as the `crypto_box` construction
does; the backend just provides the raw primitive.

### 2.2 Protocol skeleton (example mechanism, following NetMQ's reference)

The handshake state machines and the nonce prefixes are protocol-fixed; the
example mechanism implements them against `ICurveCryptoBackend`:

```text
HELLO (client):    sealed box(serverPub, clientEphemeral):
                     [client long pub 32][vouch 96][client ephemeral pub 32][client metadata]
WELCOME (server):  sealed box(clientEphemeralPub, serverEphemeral):
                     [server long pub 32][server ephemeral 80][server metadata]
INITIATE (client): sealed box(sessionKey): [vouch 96][nonce 24][server metadata]
READY (either):    sealed box(sessionKey): [metadata]

nonce prefixes (16-byte fixed + 8-byte tail, 24-byte nonce):
  "CurveZMQHELLO---", "WELCOME-", "VOUCH---", "CurveZMQINITIATE",
  "CurveZMQREADY---", "COOKIE--"
```

The session runs this sequence through `ZMechanismContext` (sealing each
command before `WriteCommandAsync`), then returns a session connection
wrapping the raw one. The protocol details (exact field layout, vouch
construction, key derivation) are copied from the maintained reference at
`zeromq/netmq` `src/NetMQ/Core/Mechanisms/CurveMechanismBase.cs`.

### 2.3 Frame-level session connection (the one genuinely new piece)

```csharp
// decrypt-on-read / encrypt-on-write, returned as ZMechanismResult.SessionConnection
internal sealed class CurveSessionConnection(IZConnection raw, byte[] boxKey) : IZConnection
{
    private Memory<byte> plainBuffer; // decrypted frames pending delivery

    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token)
    {
        // While plainBuffer is empty: read the clear frame header, read the
        // sealed body, box_open it under the session boxKey, stage it.
        // Then serve from plainBuffer - the parser's TryReadExactly sees a
        // plain stream and its frame logic is unchanged.
    }

    public ValueTask SendFrameAsync(ReadOnlyMemory<byte> frame, bool more, CancellationToken token)
    {
        // Seal the frame body, write header (clear, size = sealed length) +
        // ciphertext atomically under the raw connection's write gate.
    }
    // SendCommandAsync, SendAsync, WriteAsync delegate similarly;
    // SetFrameHandler/SetConnectionEndedHandler forward to raw.
}
```

`IZConnection` is public, so this wrapper is implementable by a user
mechanism exactly like the PLAIN example - nothing internal is required. The
parser already consumes `ReadAsync` byte-streams and never re-parses a frame
it was handed, so decrypting at frame boundaries in the wrapper is
transparent.

## 3. C# X25519 ecosystem and AOT evaluation

The CURVE mechanism needs four primitives: **X25519**, **XSalsa20-Poly1305**
(the libsodium `crypto_box_curve25519xsalsa20poly1305` construction, fixed by
the wire protocol - it cannot be substituted with ChaCha20-Poly1305),
**Ed25519** (vouch signatures), and a **CSPRNG**.

| Option | X25519 | Ed25519 | XSalsa20-Poly1305 | Native dep | AOT | Maintenance |
|---|---|---|---|---|---|---|
| BouncyCastle.Cryptography 2.7 | yes | yes | primitives only (compose XSalsa20+Poly1305) | none | **`IsAotCompatible=true`**, clean NativeAOT publishing (issues #620/#500 closed as non-library bugs) | very active, 396M downloads, 2026-07-30 |
| NSec.Cryptography 26.4 | yes | yes | **internal only, not public** | libsodium | unproven (no IsAotCompatible flag) | active, 14.9M downloads |
| Chaos.NaCl | yes | yes | yes | none | n/a | dead (last push 2021), no NuGet, `[Obsolete("Needs more testing")]` |
| libsodium bindings (Sodium.Core, or a hand-written `LibraryImport` shim) | yes | yes | yes (crypto_box) | **libsodium native** (RID `runtimes/*/native`) | `DllImport`/`LibraryImport` are NativeAOT-supported; the native package is the standard RID pattern | Sodium.Core not actively developed; a thin self-written shim over the `libsodium` native package is lower risk |
| .NET BCL | **.NET 11 only** (X25519DiffieHellman, OpenSSL/CNG-backed) | no (open proposal #63174) | no | OpenSSL/CNG (in 11) | - | in-flight |
| NetMQ's own CURVE | NaCl.Net 0.1.13 (pure managed libsodium port) | same | same | none | unverified | NaCl.Net last push 2023; NetMQ itself is the best protocol reference |

### 3.1 Verdict

- **Pure-managed, AOT-clean, all primitives: BouncyCastle.Cryptography.**
  Its one gap is that `XSalsa20Engine` and `Poly1305` ship as separate
  primitives - the `crypto_box` composition is written once in the example
  backend (roughly 40 lines: XSalsa20 with the 24-byte nonce, Poly1305 over
  the padded message, the Salsa20-derived one-time key). This is exactly the
  framing the CURVE spec's 16-byte-tag-per-message requires anyway, and
  NetMQ's `CurveMechanismBase` shows the composition.
- **Native-viable: a thin `[LibraryImport]` shim over the RID-based
  `libsodium` NuGet package**, calling `crypto_box_curve25519xsalsa20poly1305_*`
  directly. Works under NativeAOT, but adds a native runtime dependency and
  reintroduces the zero-native-dependency tension 0006 section 5 recorded.
- **Not viable today:** BCL (no X25519 before .NET 11, no Ed25519 at all),
  Chaos.NaCl (unmaintained), NSec (XSalsa20-Poly1305 not exposed).

## 4. Recommended path

Because CURVE must interoperate with libzmq, its crypto is fixed and cannot
be an abstraction over the *protocol* - only over the *primitives*. The
cleanest expression of that is what this evaluation recommends:

1. **Ship CURVE as a separately distributed, optional package**: the protocol
   skeleton from section 2 plus an `ICurveCryptoBackend` contract, with a
   **BouncyCastle-based backend** as the default implementation. This is the
   "user picks their crypto library" story: the protocol is done once, the
   user only supplies the four primitives - or just takes the BouncyCastle
   backend.
   **Implemented** (this revision): the standalone NuGet package
   `ZmqSharp.Security.Curve` (project in `ZmqSharp.Security.Curve/`) -
   `CurveMechanism` (RFC 24 handshake via the public mechanism seam),
   `CurveSessionConnection` (frame-level encrypt/decrypt),
   `ICurveCryptoBackend` + `BouncyCastleCurveCrypto`; verified end to end
   against **real libzmq (4.3.5)** in both directions - our client and server
   each complete the handshake and exchange encrypted messages with a pyzmq
   peer - plus known-vector crypto tests and ZmqSharp-to-ZmqSharp e2e.
   A user opting into CURVE references this package instead of a sample.
   Two library seams were widened by it: sends route through the session
   connection the mechanism returns (`ZSocketBase`), and `ZmtpFrameFlags` is
   public for frame-wrapping session connections.
2. **The library's own build stays CURVE-free**: no crypto dependency, no
   change to the zero-native-dependency / AOT stance, `IsAotCompatible` and
   the trim/AOT analyzers keep passing on the library itself.
3. **Validation**: the package is verified against the real wire format. An
   early hypothesis that NetMQ's CURVE is broken was investigated and
   rejected: NetMQ's CURVE is a line-faithful port of libzmq's
   `curve_client.cpp`/`curve_server.cpp` (PR #851), passes real native-libzmq
   interop tests in both directions on its Windows CI, and NaCl.Net (its
   crypto) is vector-identical to libsodium. The initial NetMQ interop
   failures were traced to two test-harness bugs (a Python greeting
   slice that silently grew the greeting to 65 bytes, and a `NetMQCertificate`
   constructor that stores `(pub, secret)` in the wrong order in 4.0.4.3).
   The genuine finding that surfaced during this is that CURVE traffic
   frames are **plain data frames** (no Command bit) whose body begins with
   the MESSAGE literal - our session connection first sent/required them as
   command frames, which libzmq rejects. The final verification is against
   real libzmq itself (pyzmq 4.3.5): both directions - our client and server
   - complete the handshake and exchange encrypted messages.
4. **Only if a built-in CURVE is later demanded** does the BouncyCastle
   dependency move into the core library, behind a feature flag - the package
   backend becomes the built-in backend unchanged.

Resolved (this revision): the example lives as a standalone package
(`ZmqSharp.Security.Curve`) rather than a `samples/` folder, so users opt in
by referencing the package - the "generic mechanism" route.

## 5. References

- 0016 - replaceable security mechanism boundary (the seam this builds on;
  section 9: CURVE session connection).
- RFC 24 / CurveZMQ (wire protocol: message sequence, nonce prefixes, vouch).
- `zeromq/netmq` - CURVE is a faithful port of libzmq's curve mechanisms
  (PR #851), interop-tested against real native libzmq on its Windows CI;
  `CurveMechanismBase.cs` is the maintained C# protocol reference.
- `bcgit/bc-csharp` - BouncyCastle.Cryptography: `X25519Agreement`,
  `Ed25519Signer`, `XSalsa20Engine`, `Poly1305`; csproj sets
  `IsAotCompatible`; AOT issues #620/#500 closed as non-library bugs.
- `dotnet/runtime` - X25519 lands in .NET 11 (`X25519DiffieHellman`); Ed25519
  tracked in issue #63174 (open).
