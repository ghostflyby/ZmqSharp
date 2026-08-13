# AGENTS.md

Engineering constraints for ZmqSharp.

## Documentation and Comments

- Documentation and code comments are written in English.

## Testing

- Test framework: xUnit; assertions: FluentAssertions.
- Test project: `ZmqSharp.Tests`, reaching internals via `InternalsVisibleTo`.

## Code Style

- No `!` null-forgiving operator; use `is {} varname` pattern matching where a non-null assertion is needed.
- Use collection literals.
- Use `System.Lock` instead of `object` as a lock.
- Use the latest C# style; API design must not be influenced by blocking-era or C-style libraries.
- Fully async pipeline: no blocking calls, `.Result`, `.Wait()`, or blocking IO APIs.
- Enable `TreatWarningsAsErrors`, and keep style consistent with `.editorconfig` + `dotnet format`.

## AOT

- Full Native AOT support: no runtime reflection or dynamic code generation; use source generators for serialization/metadata needs.
- The library enables AOT compatibility checks via `IsAotCompatible`, which turns on the AOT, trim, and single-file analyzers; warnings are errors.

## Namespaces

- The top-level `ZmqSharp` namespace holds the base public API: socket factory and surfaces, the message model, and configuration. A single `using ZmqSharp;` covers basic usage.
- Sub-namespaces represent domain-specific feature areas: `ZmqSharp.Transports` (custom transports), `ZmqSharp.Zmtp` (ZMTP wire codec), `ZmqSharp.Security` (security mechanisms), `ZmqSharp.Patterns` (socket-composition seams: dispatch/inbound policies and `ZSocketType` for custom socket types).
- Internal API namespaces mirror the directory layout. See `docs/impl/0018-namespace-and-directory-organization.md`.

## Documents

- Design documents live in `docs/impl/`, numbered incrementally (0001, 0002...), each with a status (draft / accepted / superseded).
