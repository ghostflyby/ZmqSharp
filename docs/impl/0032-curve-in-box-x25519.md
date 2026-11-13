# 0032 - CURVE X25519 from the in-box crypto on net11.0

Status: accepted
Date: 2026-11-13

.NET 11 ships `System.Security.Cryptography.X25519DiffieHellman`. Since 0027,
BouncyCastle's only remaining role in the CURVE package was the RFC 7748
`X25519` type; the Salsa20 core and Poly1305 are hand-written and stay
hand-written, because the BCL has no XSalsa20 and its `ChaCha20Poly1305` is
not wire-compatible with libsodium's `crypto_secretbox`. This document records
switching the X25519 call site to the in-box implementation on net11.0 and
making the BouncyCastle dependency conditional per target framework.

## 1. Decision

Prefer the in-box algorithm where it exists:

- net11.0: `CurveX25519` (the new internal seam behind `CurveCrypto`) imports
  the raw 32-byte secret with `X25519DiffieHellman.ImportPrivateKey`, exports
  the public key with `ExportPublicKey`, and computes the shared secret with
  the span overload of `DeriveRawSecretAgreement`. PKCS#8/PEM and the
  `X25519DiffieHellman`-typed overloads are not used: the CURVE keys are raw
  Z85-decoded 32-byte values and stay that way.
- net10.0: the BouncyCastle `Rfc7748.X25519` calls are unchanged, selected by
  `#if` in the same seam class.
- The csproj declares `BouncyCastle.Cryptography` for every target except
  net11.0 (`Condition="'$(TargetFramework)' != 'net11.0'"`), mirroring the
  `#if !NET11_0_OR_GREATER` branch of `CurveX25519`, which NuGet flows into
  the per-framework dependency groups of the nupkg: net11.0 consumers pull no
  crypto package at all.

The rest of the package is untouched. The frame hot path
(`CurveFrameCodec`) never calls X25519 - traffic frames are
`SecretBox`-sealed under the precomputed session key - so the in-box path's
one `X25519DiffieHellman` instance per call is a handshake-only allocation
(0027's zero-allocation goal is about the traffic path and holds).

## 2. Behavior parity

Both backends are locked by the same libsodium/PyNaCl known vectors and the
NetMQ/PyZMQ interop tests, which run on both target frameworks.

Low-order peer keys: libsodium's `crypto_scalarmult` rejects a peer key whose
shared secret is all zero; BouncyCastle's `CalculateAgreement` reports it as
failure. The in-box behavior is provider-dependent - OpenSSL on macOS throws
`CryptographicException` from the provider call, but a provider that surfaces
the all-zero secret instead would otherwise slip through. `CurveX25519`
therefore catches the provider exception (rethrowing with the existing
message) and explicitly checks the output for all zeros, so every backend on
every platform rejects low-order keys the same way. A dedicated known-vector
test locks this on both frameworks.

Platforms where `X25519DiffieHellman.IsSupported` is false (no CNG, no
OpenSSL) fail with the BCL's `PlatformNotSupportedException` on net11.0:
the package deliberately carries no fallback backend on that target.

## 3. History

- 0017 chose BouncyCastle as the crypto backend when no in-box X25519 existed.
- 0027 reduced that dependency to X25519 only and hand-wrote the
  Salsa20/Poly1305 cores; this document supersedes 0027's "BouncyCastle is
  used only for X25519" detail - on net11.0 it is not used at all.
