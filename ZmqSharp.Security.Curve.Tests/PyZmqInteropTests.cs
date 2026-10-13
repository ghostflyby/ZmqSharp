using System.Buffers;
using System.Diagnostics;
using Xunit;

namespace ZmqSharp.Security.Curve.Tests;

/// <summary>
/// Cross-library CURVE interop against Python pyzmq (libzmq): launches the
/// preset <c>pyzmqInterop.py</c> peer (CURVE server on a PAIR socket), then
/// connects a ZmqSharp client and echoes three frames both ways - proving the
/// RFC 25 wire bytes against an independent implementation (libsodium).
///
/// Off by default: set <c>ZMQSHARP_PYZMQ_INTEROP=1</c> to enable. The python
/// interpreter is probed for pyzmq (override the executable with
/// <c>ZMQSHARP_PYTHON</c>, otherwise python3/python on PATH); anything
/// missing skips the test instead of failing it.
/// </summary>
public sealed class PyZmqInteropTests
{
    private const string EnableVariable = "ZMQSHARP_PYZMQ_INTEROP";
    private const string PythonVariable = "ZMQSHARP_PYTHON";

    private static readonly string ScriptPath =
        Path.Combine(AppContext.BaseDirectory, "pyzmqInterop.py");

    private static readonly byte[] ServerPublicKey = Convert.FromHexString(
        "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");

    [Fact(Timeout = 60_000)]
    public async Task ExchangesFrames_WithPyZmq()
    {
        if (Environment.GetEnvironmentVariable(EnableVariable) != "1")
            Assert.Skip($"off by default; set {EnableVariable}=1 to run against python/pyzmq");

        var python = ResolvePython();
        if (python is null) Assert.Skip("no python interpreter found (tried ZMQSHARP_PYTHON, python3, python)");
        if (!HasPyZmq(python)) Assert.Skip($"{python} cannot import zmq (pyzmq not installed)");

        var token = TestContext.Current.CancellationToken;
        var port = GetFreePort();
        using var peer = StartPeer(python, port);

        // The peer prints "READY <bind>" once the socket is bound; anything
        // else (or nothing) means it died before we can connect.
        var ready = await peer.StandardOutput.ReadLineAsync(token)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(15), token);
        Assert.True(ready is not null && ready.StartsWith("READY "), $"peer did not become ready: {ready}");

        await using var local = new ZPairSocket(new ZSocketOptions
        {
            Security = new ZSecurityOptions
            {
                Mechanism = new CurveMechanism(
                    Convert.FromHexString("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"),
                    ServerPublicKey)
            }
        });
        await local.ConnectAsync($"tcp://127.0.0.1:{port}", token);

        byte[][] payloads = [[.. "interop-1"u8], [.. "interop-2"u8], [.. "interop-3"u8]];

        foreach (var payload in payloads)
        {
            await local.SendAsync(ZMessage.FromOwned([.. payload]), token);
            using var echo = await local.Messages.ReadAsync(token);
            Assert.Equal([.. payload], echo[0].ToSequence().ToArray());
        }

        // The peer exits 0 after printing the done line; a non-zero exit means
        // the echo verification failed on the pyzmq side.
        using var exit = peer.WaitForExitAsync(token).WaitAsync(TimeSpan.FromSeconds(15), token);
        await exit;
        Assert.True(peer.ExitCode == 0, $"peer exited {peer.ExitCode}");
        Assert.Contains("PYZMQ-ECHO-DONE", await peer.StandardOutput.ReadToEndAsync(token));
    }

    private static Process StartPeer(string python, int port)
    {
        var info = new ProcessStartInfo
        {
            FileName = python,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add(ScriptPath);
        info.ArgumentList.Add("--bind");
        info.ArgumentList.Add($"tcp://127.0.0.1:{port}");
        var peer = Process.Start(info);
        Assert.True(peer is not null, $"failed to start {python}");
        return peer;
    }

    private static string? ResolvePython()
    {
        var overridePath = Environment.GetEnvironmentVariable(PythonVariable);
        IEnumerable<string> candidates = overridePath is { } overridden
            ? [overridden]
            : ["python3", "python"];

        foreach (var candidate in candidates)
            if (Runs(candidate, ["-c", "pass"])) return candidate;

        return null;
    }

    private static bool HasPyZmq(string python) => Runs(python, ["-c", "import zmq"]);

    private static bool Runs(string executable, IReadOnlyList<string> arguments)
    {
        try
        {
            var info = new ProcessStartInfo
            {
                FileName = executable,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (var argument in arguments) info.ArgumentList.Add(argument);

            using var probe = Process.Start(info);
            if (probe is null) return false;
            probe.WaitForExit(10_000);
            return probe is { HasExited: true, ExitCode: 0 };
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static int GetFreePort()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
