namespace ZmqSharp.Sockets;

internal sealed class RequestHandlerSlot
{
    private readonly Lock _gate = new();
    private Func<ZRequestContext, CancellationToken, ValueTask>? _handler;

    public void Set(Func<ZRequestContext, CancellationToken, ValueTask> value)
    {
        ArgumentNullException.ThrowIfNull(value);
        lock (_gate) _handler = value;
    }

    public ValueTask InvokeAsync(ZRequestContext context, CancellationToken token)
    {
        Func<ZRequestContext, CancellationToken, ValueTask>? current;
        lock (_gate) current = _handler;
        return current?.Invoke(context, token) ?? ValueTask.CompletedTask;
    }
}
