namespace ZmqSharp.Sockets;

internal sealed class RequestHandlerSlot
{
    private readonly Lock gate = new();
    private Func<ZRequestContext, CancellationToken, ValueTask>? handler;
    public void Set(Func<ZRequestContext, CancellationToken, ValueTask> value)
    {
        ArgumentNullException.ThrowIfNull(value);
        lock (gate) handler = value;
    }
    public ValueTask InvokeAsync(ZRequestContext context, CancellationToken token)
    {
        Func<ZRequestContext, CancellationToken, ValueTask>? current;
        lock (gate) current = handler;
        return current?.Invoke(context, token) ?? ValueTask.CompletedTask;
    }
}
