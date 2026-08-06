namespace ZmqSharp.Zmtp;

/// <summary>Receive materialization mode.</summary>
public enum ZReceiveMode
{
    /// <summary>Borrowed: zero copy, valid only during the callback, no owned message is produced.</summary>
    Borrowed,

    /// <summary>Pooled: memory rented from a pool; returned when the message is disposed.</summary>
    Pooled,

    /// <summary>Owned: GC-allocated, never touches a pool, may be kept permanently.</summary>
    Owned,
}
