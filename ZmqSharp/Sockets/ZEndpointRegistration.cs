namespace ZmqSharp.Sockets;

/// <summary>Owns endpoint shutdown and its completion independently of routing membership.</summary>
internal sealed class ZEndpointRegistration(
    IDisposable resource,
    object? endpoint,
    Type? transport,
    string? address,
    CancellationToken lifetime,
    CancellationToken attempt = default,
    Action? abort = null)
{
    private readonly Lock _gate = new();
    private readonly CancellationTokenSource _cancellation = CancellationTokenSource.CreateLinkedTokenSource(lifetime, attempt);
    private Task? _stop;

    public object? Endpoint { get; } = endpoint;
    public Type? Transport { get; } = transport;
    public string? Address { get; } = address;
    public CancellationToken Token => _cancellation.Token;
    public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void RequestStop()
    {
        lock (_gate) _stop ??= StopAsync();
    }

    private async Task StopAsync()
    {
        var canceled = _cancellation.CancelAsync();
        try
        {
            if (abort is not null) abort();
            else resource.Dispose();
        }
        finally
        {
            await canceled;
        }
    }

    public async Task FinishAsync()
    {
        Task stopping;
        lock (_gate) stopping = _stop ??= StopAsync();
        try
        {
            await stopping;
        }
        finally
        {
            _cancellation.Dispose();
        }
    }

    public void Complete(Exception? failure)
    {
        if (failure is null) Completion.TrySetResult();
        else Completion.TrySetException(failure);
    }
}
