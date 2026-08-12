namespace ZmqSharp.Security;

/// <summary>
/// One handshake command frame read by a mechanism session: the short-string
/// command name and the body after the name. Both memories are borrowed from
/// the <see cref="ZMechanismContext"/> scratch buffer and stay valid only until
/// the next read on that context - the same lifetime rule as the traffic
/// parser's borrowed frames. A session that must retain the body (e.g. the
/// peer's READY metadata) copies it.
/// </summary>
public readonly struct ZMechanismCommand(ReadOnlyMemory<byte> name, ReadOnlyMemory<byte> arguments)
{
    /// <summary>Command name without the length prefix (e.g. "READY", "WELCOME").</summary>
    public ReadOnlyMemory<byte> Name { get; } = name;

    /// <summary>Command body after the name (READY metadata, ERROR reason, ...).</summary>
    public ReadOnlyMemory<byte> Arguments { get; } = arguments;
}
