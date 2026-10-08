using System.Net;
using System.Net.Sockets;
using ZmqSharp;
using ZmqSharp.Security.Curve;

foreach (var ipc in (bool[])[false, true])
    foreach (var curve in (bool[])[false, true])
    {
        var backend = new BouncyCastleCurveCrypto();
        backend.GenerateKeyPair(out var pub, out var secret);
        backend.GenerateKeyPair(out _, out var clientSecret);
        var received = 0;
        var target = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ZPairSocket(new ZSocketOptions
        {
            ReceiveSurface = ZReceiveSurface.Callback,
            Security = curve ? new ZSecurityOptions { Mechanism = new CurveMechanism(backend, secret) } : ZSecurityOptions.Null
        });
        server.OnFrame += (frame, _) =>
        {
            if (frame.Count != 1 || frame[0].Memory.Length != 64) throw new InvalidOperationException();
            if (Interlocked.Increment(ref received) == Volatile.Read(ref target)) done.TrySetResult();
            return true;
        };
        await using var client = new ZPairSocket(new ZSocketOptions
        {
            ReceiveSurface = ZReceiveSurface.Callback,
            Security = curve ? new ZSecurityOptions { Mechanism = new CurveMechanism(backend, clientSecret, pub) } : ZSecurityOptions.Null
        });
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var address = ipc ? $"ipc://{Path.Combine(Path.GetTempPath(), $"zmq-measure-{Guid.NewGuid().ToString("N")[..12]}.sock")}" : $"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        listener.Stop();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await server.BindAsync(address, timeout.Token);
        await client.ConnectAsync(address, timeout.Token);
        ReadOnlyMemory<byte> bytes = new byte[64];
        const int count = 10000;
        async Task Batch(int n)
        {
            done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref target, Volatile.Read(ref received) + n);
            for (var i = 0; i < n; i++) await client.SendAsync(bytes, timeout.Token);
            await done.Task.WaitAsync(timeout.Token);
        }
        await Batch(1000);
        var samples = new List<double>();
        for (var i = 0; i < 5; i++)
        {
            var before = GC.GetTotalAllocatedBytes(precise: true);
            await Batch(count);
            var after = GC.GetTotalAllocatedBytes(precise: true);
            samples.Add((after - before) / (double)count);
        }
        Console.WriteLine($"{(ipc ? "IPC" : "TCP")} {(curve ? "CURVE" : "NULL")} bytes/message: {string.Join(", ", samples.Select(v => v.ToString("F2")))}; median={samples.Order().ElementAt(2):F2}");
    }
