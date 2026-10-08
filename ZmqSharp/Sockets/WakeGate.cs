namespace ZmqSharp.Sockets;

/// <summary>Capture before checking the readable level to avoid lost wakeups.</summary>
internal sealed class WakeGate
{
    private readonly Lock gate = new();
    private TaskCompletionSource current = NewGate();
    private static TaskCompletionSource NewGate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Capture() { lock (gate) return current.Task; }
    public void Wake()
    {
        lock (gate)
        {
            current.TrySetResult();
            current = NewGate();
        }
    }
}
