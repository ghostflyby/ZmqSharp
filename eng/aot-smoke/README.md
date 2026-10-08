# Native AOT and external-contract smoke

Publish and execute the current source as an external consumer with no access to
internals:

```sh
dotnet publish eng/aot-smoke/Smoke.csproj -c Release -r osx-arm64 -o /tmp/zmq-aot
/tmp/zmq-aot/Smoke
```

Choose the matching native RID on other hosts. The executable exercises a custom
public mechanism/codec and byte-only transport adapter, ZPeer, endpoint cleanup,
NULL TCP/IPC and CURVE with reverse topology over TCP/IPC. Warnings are errors.

Optional local PLAIN interoperability against pyzmq's libzmq:

```sh
python3 -m pip install --only-binary=:all: pyzmq==27.1.0
python3 eng/aot-smoke/verify-plain-interop.py /tmp/zmq-aot/Smoke
```

The asyncio harness checks both local security roles with both bind and connect.
It runs independently of the .NET test suite; no Python dependency is required to
build, test or publish the library.

## Continuous validation

The `External consumer validation` workflow is called by CI for pull requests,
pushes to `main`, and manual CI runs. It can also be run directly using its
`workflow_dispatch` entry point. It uses Ubuntu with .NET 10, Python 3.12 and
pyzmq 27.1.0, installs `clang` and `zlib1g-dev`, and publishes a `linux-x64`
Native AOT executable from the current source. Both the native smoke and all four
PLAIN exchanges must pass using that same executable.

The workflow also builds the allocation measurement consumer and verifies both
engineering projects' formatting. Allocation measurements remain manual. A
20-minute job timeout bounds validation; publish and execution logs are uploaded
even on failure. Failure makes CI fail, so the release workflow's existing
same-commit CI check includes external consumer validation.

The published-package smoke remains a separate manual workflow. Linux is the
initial automated Native AOT platform; local macOS validation recorded in design
document 0030 remains separate evidence, not proof of a Linux workflow run.
