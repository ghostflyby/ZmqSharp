# Real wire allocation measurement

Run in an isolated Release process:

```sh
dotnet run --project eng/measure-wire-allocations -c Release
```

To compare an earlier checkout, pass `-p:LibraryRoot=/absolute/path/to/checkout`.
It must provide compatible public socket/message/backend construction APIs.
Use a separate process for each checkout; no traffic test should run in-process.

The harness measures TCP and real IPC, NULL and CURVE, with 64-byte borrowed
frames and callback reception. After 1,000 warmup frames, it reports five batches
of 10,000 frames and their median process-wide allocated bytes per delivered
message, including both peers and BCL async I/O. Thread scheduling affects these
numbers, so they are comparative evidence rather than a deterministic allocation
gate. The allocation test project separately enforces library hot-path budgets.

The external consumer validation workflow builds this project in Release and
checks its formatting on Linux. It does not run these measurements or enforce a
threshold on their results; measurements remain an explicit manual step.
