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
- New `HasValue` and `Value` (`object?`) members complete the pattern.
  `Value` is the boxed active case (or null for a `default` instance); the
  compiler prefers `TryGetValue`/`HasValue`, so `Value` is never touched on
  the discrimination paths.

The shipped implicit conversion operators stay: a user-defined operator
shadows the union conversion with identical behavior (both call the same
constructor), and removing them would be a binary-breaking API change.

`UnionAttribute` exists only in the net11.0 runtime, so the attribute is
compiled under `#if NET11_0_OR_GREATER`. Everything else - the constructors,
the `TryGetValue` overloads, the `Value`/`HasValue` members - is
unconditional: both targets declare an identical member surface, so one pair
of PublicAPI files covers both. On `net10.0` the attribute is absent, the
compiler sees plain structs, and consumers work in non-union mode: construct
from a case, discriminate through `TryGetValue`, convert through the implicit
operators, with `Value`/`HasValue` as ordinary members.

## 3. Union behaviors gained

- `message is ZSingleMessage single`, `frame is ZSegments segments` - a type
  pattern tests the active case through `TryGetValue`, with no boxing.
- Property patterns apply to the unwrapped case:
  `frame is ZSegment { IsBorrowed: false }`.
- A switch over both case types is exhaustive without a fallback arm; a
  `null` arm covers the value-less `default` instance (`Value` is `object?`,
  so its default null state is "maybe null").
- `is null` on a union value succeeds when there is no case value, i.e. for
  `default` instances.

## 4. Internal usage

The library discriminates frames and messages through the `TryGetValue`
overloads on every target. `is` over a union lowers to exactly those calls,
so the unconditional call path keeps both builds identical while union
matching stays a consumer-side capability.

## 5. Public API additions

`ZFrame.Value`, `ZFrame.HasValue`, `ZMessage.Value`, `ZMessage.HasValue`
(declared in `PublicAPI.Unshipped.txt`).

## 6. Test plan

- Type patterns identify the active case at both levels; property
  subpatterns apply to the unwrapped case.
- A switch over both case types plus `null` is exhaustive; `default`
  instances take the `null` arm.
- `Value` returns the boxed active case; `HasValue` is false and `Value` is
  null for `default` instances.
- The union tests compile only for `net11.0`; the rest of the suites run on
  both targets, so the net10.0 build is exercised without union behaviors.
