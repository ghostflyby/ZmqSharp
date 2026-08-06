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
- The library enables AOT compatibility checks (`IsAotCompatible`) and is validated by a `PublishAot` build of `ZmqSharp.AotSmoke`.

## Namespaces

- Organized by layer: `ZmqSharp.Messages` / `ZmqSharp.Zmtp` / `ZmqSharp.Transports` / `ZmqSharp.Patterns`.

## Documents

- Design documents live in `docs/impl/`, numbered incrementally (0001, 0002...), each with a status (draft / accepted / superseded).
