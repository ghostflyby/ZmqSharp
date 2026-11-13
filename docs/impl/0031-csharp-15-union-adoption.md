# 0031 - C# 15 Union Adoption for Frames and Messages

Status: accepted
Date: 2026-11-13

.NET 11 / C# 15 ships union types. 0005 section 6 anticipated this migration:
the split-storage union-like pattern maps onto a real union without changing
the layout. This document records how `ZFrame` and `ZMessage` adopt the union
behaviors while keeping the 0005 storage strategy. The repository builds on
the .NET 11 SDK (`global.json` 11.0.100) and multi-targets the packable and
test projects on `net10.0;net11.0`: union mode is exclusive to `net11.0`,
while `net10.0` consumers keep the complete non-union surface.

## 1. Motivation

0005 shaped `ZFrame` (Contiguous / NonContiguous) and `ZMessage` (Single /
Multi) as hand-rolled unions: one constructor per case, `TryGetValue`
overloads per case, implicit conversions from the case types. Until now those
were conventions only - consumers had to call `TryGetValue` and the compiler
could not verify that a switch handled every case.

C# 15 recognizes a type decorated with
`System.Runtime.CompilerServices.UnionAttribute` as a union type and gives it
union behaviors on top of the members it already has: union conversions from
the case types, pattern matching that unwraps the active case, and
exhaustive switches over the case types.

## 2. Hand-expressed unions, not union declarations

The `union` declaration syntax is not used: it lowers to a struct with a
single `object?` field, which would box every case and abandon the 0005
split-per-case storage (each nullable case field holds its struct inline, so
the hot path stays allocation-free). Instead both union roots keep their
layout and carry `[Union]`, which the language calls a *union type* whose
members may be hand-declared:

- The existing single-parameter constructors are the union *creation
  members*; their parameter types are the case types
  (`ZSegment`/`ZSegments` for `ZFrame`, `ZSingleMessage`/`ZMultiMessage` for
  `ZMessage`).
- The existing `TryGetValue` overloads are the *non-boxing union access
  pattern*: pattern matching compiles to those calls, so discrimination
  stays allocation-free.
- A new `Value` (`object`) member completes the pattern. The compiler
  prefers the `TryGetValue` overloads for discrimination, so `Value` is
  never touched on those paths.

The shipped implicit conversion operators stay: a user-defined operator
shadows the union conversion with identical behavior (both call the same
constructor), and removing them would be a binary-breaking API change.

`UnionAttribute` exists only in the net11.0 runtime, so the attribute is
compiled under `#if NET11_0_OR_GREATER`. Everything else - the constructors,
the `TryGetValue` overloads, the `Value` member - is unconditional: both
targets declare an identical member surface, so one pair of PublicAPI files
covers both. On `net10.0` the attribute is absent, the compiler sees plain
structs, and consumers work in non-union mode: construct from a case,
discriminate through `TryGetValue`, convert through the implicit operators,
with `Value` as an ordinary member.

## 3. Union behaviors gained

- `message is ZSingleMessage single`, `frame is ZSegments segments` - a type
  pattern tests the active case through `TryGetValue`, with no boxing.
- Property patterns apply to the unwrapped case:
  `frame is ZSegment { IsBorrowed: false }`.
- A switch over both case types is exhaustive without a fallback arm, and
  the coverage is total at runtime: the case types are non-nullable, so
  `Value`'s null state is "not null", and a `default` instance lands in the
  single/contiguous arm rather than falling through.
- `is null` is always false for a union value; "no message" is expressed as
  `ZMessage?` at the usage site.

## 4. Default instances

Every custom union gives `default` a meaning instead of a value-less state:
`default(ZMessage)` is the single case holding `default(ZSingleMessage)`,
and `default(ZFrame)` is the contiguous case holding `default(ZSegment)` -
an empty frame. `default(ZMultiMessage)` and `default(ZSegments)` are empty
tables: `Count` is 0, indexing throws `ArgumentOutOfRangeException`, and
enumeration and `Dispose` are no-ops. `TryGetValue` returns true for the
single/contiguous case on a default instance, and `Value` is the boxed
default case - never null.

This keeps the whole surface total (no member throws
`NullReferenceException` on a default value) and makes exhaustive switches
sound at runtime: `default` lands in the empty-case arm instead of falling
through. A missing message is expressed as `ZMessage?` rather than a
value-less `ZMessage`.

## 5. Internal usage

The library discriminates frames and messages through the `TryGetValue`
overloads on every target. `is` over a union lowers to exactly those calls,
so the unconditional call path keeps both builds identical while union
matching stays a consumer-side capability.

## 6. Public API additions

`ZFrame.Value`, `ZMessage.Value` (declared in `PublicAPI.Unshipped.txt`).

## 7. Test plan

- Type patterns identify the active case at both levels; property
  subpatterns apply to the unwrapped case.
- A switch over both case types is exhaustive with no fallback arm;
  `default` instances take the single arm.
- `Value` returns the boxed active case and is never null.
- Default instances are the empty case at every level: `TryGetValue`
  succeeds for single/contiguous, `Count`/enumeration stay consistent, the
  multi cases are empty tables, and `Dispose` is a no-op.
- The union tests compile only for `net11.0`; the rest of the suites run on
  both targets, so the net10.0 build is exercised without union behaviors.
