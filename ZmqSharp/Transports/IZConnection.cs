namespace ZmqSharp.Transports;

/// <summary>A byte connection allowing one reader and one writer concurrently.</summary>
public interface IZConnection : IZByteReader, IZByteWriter, IDisposable
{
    /// <summary>Idempotently aborts I/O. Await outstanding operations before disposing remaining resources.</summary>
    void Abort();
}
