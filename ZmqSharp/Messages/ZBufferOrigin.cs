namespace ZmqSharp.Messages;

/// <summary>Memory origin of a message segment.</summary>
public enum ZBufferOrigin
{
    /// <summary>Memory rented from a MemoryPool; returned to the pool on Dispose.</summary>
    Pooled,

    /// <summary>Memory owned by the caller or the GC; Dispose returns nothing to any pool.</summary>
    Owned,
}
