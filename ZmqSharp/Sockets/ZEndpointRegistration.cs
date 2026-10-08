namespace ZmqSharp.Sockets;

/// <summary>Owns endpoint shutdown and its completion independently of routing membership.</summary>
internal sealed class ZEndpointRegistration(
    IDisposable resource, object? endpoint, Type? transport, string? address,
    CancellationToken lifetime, CancellationToken attempt = default)
{
    private readonly Lock gate = new();
    private readonly CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(lifetime, attempt);
    private Task? stop;

    public object? Endpoint { get; } = endpoint;
    public Type? Transport { get; } = transport;
    public string? Address { get; } = address;
    public CancellationToken Token => cancellation.Token;
    public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void RequestStop()
    {
        lock (gate) stop ??= StopAsync();
    }

    private async Task StopAsync()
    {
        try
        {
            await cancellation.CancelAsync();
        }
        finally
        {
            resource.Dispose();
        }
    }

    public async Task FinishAsync()
    {
        Task stopping;
        lock (gate) stopping = stop ??= StopAsync();
        try { await stopping; }
        finally { cancellation.Dispose(); }
    }

    public void Complete(Exception? failure)
    {
        if (failure is null) Completion.TrySetResult();
        else Completion.TrySetException(failure);
    }
}
