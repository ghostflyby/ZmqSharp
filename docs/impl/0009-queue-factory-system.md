# 0009 - Declarative Channel Factory System

Status: draft
Date: 2026-08-10

Defines the declaration-style channel construction system for
`ZQueueSocket<TSocket>`: a `ZQueueFactory` strategy type with bounded and
unbounded implementations, mirroring the receive policy system (0003/0008).
Channel configuration moves from the four scalar `ZQueueSocketOptions`
fields to two factory properties; BCL channel options convert implicitly
into a factory.

This document supersedes the channel-configuration parts of 0002, 0004, 0006,
and 0008 that described `ReceiveCapacity`, `ReceiveFullMode`, `SendCapacity`,
and `SendFullMode`.

## 1. Motivation

The receive and outbound channels of the queue socket were configured through
scalar fields on `ZQueueSocketOptions`. That shape cannot express bounded
versus unbounded channels, thread-safety flags, or synchronous-continuation
preferences, and it duplicated BCL's own `BoundedChannelOptions` /
`UnboundedChannelOptions`. The factory system makes channel construction a
declarative strategy in the same style as `IZReceivePolicy.Decide`, and keeps
the mandatory drop-reclamation hook (0006 section 2.2) out of user hands.

## 2. Public Surface

```csharp
public interface IZQueueFactory<TOptions>
    where TOptions : ChannelOptions
{
    Channel<ZMessage> Create(Action<ZMessage> itemDropped);
}

public abstract class ZQueueFactory : IZQueueFactory<ChannelOptions>
{
    public abstract Channel<ZMessage> Create(Action<ZMessage> itemDropped);

    public static implicit operator ZQueueFactory(BoundedChannelOptions options);
    public static implicit operator ZQueueFactory(UnboundedChannelOptions options);
}

public sealed class ZBoundedQueueFactory : ZQueueFactory, IZQueueFactory<BoundedChannelOptions>
{
    public ZBoundedQueueFactory(int capacity,
        BoundedChannelFullMode fullMode = BoundedChannelFullMode.Wait,
        bool singleWriter = true,
        bool allowSynchronousContinuations = false);
    public ZBoundedQueueFactory(BoundedChannelOptions options);
}

public sealed class ZUnboundedQueueFactory : ZQueueFactory, IZQueueFactory<UnboundedChannelOptions>
{
    public ZUnboundedQueueFactory(bool singleWriter = true, bool allowSynchronousContinuations = false);
    public ZUnboundedQueueFactory(UnboundedChannelOptions options);
}
```

The channel element type is fixed to `ZMessage`; the generic type parameter
names the channel-option type the factory is configured by
(`BoundedChannelOptions` or `UnboundedChannelOptions`), so the factory
strategy is typed by its configuration source rather than by the message
type. The hierarchy is a chain: `ZBoundedQueueFactory` /
`ZUnboundedQueueFactory` inherit the non-generic `ZQueueFactory` base, which
implements the strategy contract closed at the common `ChannelOptions` base,
and each concrete factory additionally implements the contract closed at its
own option type. A bounded factory is therefore consumable both as
`IZQueueFactory<ChannelOptions>` (through the base) and as
`IZQueueFactory<BoundedChannelOptions>` (directly).

`ZQueueSocketOptions`:

```csharp
public ZQueueFactory ReceiveQueueFactory { get; init; } = new ZBoundedQueueFactory(16);
public ZQueueFactory? SendQueueFactory { get; init; }   // null = outbound disabled
```

The four former scalar fields (`ReceiveCapacity`, `ReceiveFullMode`,
`SendCapacity`, `SendFullMode`) are removed.

### 2.1 Why a non-generic base plus implicit conversion

C# requires a user-defined conversion operator to be declared on the source
or the target type, and BCL options cannot carry one. A target type that is a
generic interface is also invisible to operator lookup, so a purely generic
`IZQueueFactory<TOptions>` property could never accept
`new BoundedChannelOptions(16)` implicitly. The non-generic `ZQueueFactory`
base hosts both conversion operators, so this compiles:

```csharp
var options = new ZQueueSocketOptions
{
    ReceiveQueueFactory = new BoundedChannelOptions(16) { SingleWriter = true },
};
```

## 3. Factory Contract

- A factory is stateless and thread-safe; the same instance may create many
  channels (`OnPeerConnected` calls `ReceiveQueueFactory.Create` per peer).
- `Create` receives the library's mandatory `itemDropped` hook
  (`static message => message.Dispose()`) and wires it into
  `Channel.CreateBounded`. A user factory cannot bypass the reclamation path;
  an unbounded channel has no drop concept and ignores the hook (explicit
  drains still reclaim, 0006 section 2.2).
- `SingleReader` is always forced to `true` in the factory's options
  snapshot: the library is the sole reader of every channel it builds, so the
  flag is an implementation guarantee, not a caller choice.
- `SingleWriter` is preserved from the caller's options. The receive side is
  SPSC (single writer per peer queue, 0004 constraint 5); the outbound channel
  is a shared producer surface, so a caller enabling it must pass
  `SingleWriter = false` (the convenience constructor's default of `true` is
  for the receive-side shape).
- Options are copied into the factory-owned snapshot at construction; later
  mutation of the caller's instance does not affect already-built factories,
  and the same factory's repeated `Create` calls are consistent.

## 4. Unbounded and the 0004 HWM Constraint

`ZUnboundedQueueFactory` is opt-in. An unbounded receive queue gives up the
per-peer peak-memory bound of 0004 constraint 3 ("queues are always
bounded"), so 0004 is revised: bounded is the default and the only shape
guaranteeing HWM-controlled peak memory; unbounded is an explicit choice that
trades that bound for never blocking.

## 5. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | `ZQueueFactory` non-generic base implements `IZQueueFactory&lt;ChannelOptions&gt;` and hosts the implicit conversions; each concrete factory additionally implements the contract closed at its own option type | C# requires the operator on source or target, and a generic-interface target is invisible to operator lookup; the element type is fixed to `ZMessage`, so only the configuration type varies and it does so through inheritance |
| D2 | `Create(Action<T> itemDropped)` takes the reclamation hook as an argument | Mandatory drop disposal stays a library responsibility (0006 2.2); a user factory cannot bypass it |
| D3 | `SingleReader` forced true; `SingleWriter` preserved | The library is the sole reader; the outbound channel is a shared producer surface, so single-writer is a per-use decision |
| D4 | Options copied at construction | `BoundedChannelOptions` is a mutable class without a clone; the snapshot keeps factories consistent and immune to later mutation |
| D5 | The convenience constructor takes the BCL `BoundedChannelFullMode` directly; no ZmqSharp full-mode enum exists | Users configuring via `BoundedChannelOptions` and via the convenience constructor use the same BCL enum, removing a redundant parallel type |
| D6 | Unbounded is opt-in and revises 0004 constraint 3 | Default remains bounded with HWM control; unbounded trades the peak-memory bound for never blocking |

## 6. Non-Goals

- Per-peer send queues (0004 D2): this document only makes the factory
  reusable; the outbound channel remains one socket-level queue drained by
  one pump.
- Drop diagnostics counters or events (0006 section 2.2).
