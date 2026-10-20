namespace ZmqSharp.Sockets;

/// <summary>Capture before checking the readable level to avoid lost wakeups.</summary>
internal sealed class WakeGate
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _current = NewGate();
    private static TaskCompletionSource NewGate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Capture()
    {
        lock (_gate) return _current.Task;
    }

    public void Wake()
    {
        lock (_gate)
        {
            _current.TrySetResult();
            _current = NewGate();
        }
    }
}
