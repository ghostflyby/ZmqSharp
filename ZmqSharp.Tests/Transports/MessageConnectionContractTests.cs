using System.Buffers;
using Xunit;
using ZmqSharp.Transports;
using ZmqSharp.Zmtp;

namespace ZmqSharp.Tests.Transports;

/// <summary>
/// Contract suite for the message-mode connection seam (0033), mirroring the
/// byte-contract suites (ZSocketConnectionTests for delivery shape,
/// RuntimeLifecycleTests for abort/lifetime discipline). The scripted fake is
/// deterministic: it serves whole messages, reuses one receive buffer (so
/// borrowed lifetimes are observable), and records each
/// WriteMessageAsync as one gathered message.
/// </summary>
public sealed class MessageConnectionContractTests
{
    [Fact]
    public async Task ReadMessageAsync_DeliversWholeMessagesWithoutPartials()
    {
        using var connection = new ScriptedMessageConnection(
        [
            [.. "alpha"u8],
            [.. "beta"u8],
            [.. "gamma"u8],
        ], eofWhenExhausted: true);
        var token = TestContext.Current.CancellationToken;

        // Each read's memory is borrowed only until the next read, so a
        // consumer snapshots before reading on - exactly as with the parser's
        // borrowed frames.
        var first = (await connection.ReadMessageAsync(1024, token)).Value.ToArray();
        var second = (await connection.ReadMessageAsync(1024, token)).Value.ToArray();
        var third = (await connection.ReadMessageAsync(1024, token)).Value.ToArray();

        Assert.Equal([.. "alpha"u8], first);
        Assert.Equal([.. "beta"u8], second);
        Assert.Equal([.. "gamma"u8], third);

        // The script exhausted: the next read is a clean EOF, not a hang.
        Assert.Null(await connection.ReadMessageAsync(1024, token));
    }

    [Fact]
    public async Task EmptyMessage_IsDistinctFromEof()
    {
        // The core semantic a byte view cannot express: an empty memory is a
        // valid zero-length message; only null means the peer closed.
        using var connection = new ScriptedMessageConnection(
        [
            [],
            [.. "x"u8],
        ], eofWhenExhausted: true);
        var token = TestContext.Current.CancellationToken;

        var empty = await connection.ReadMessageAsync(1024, token);
        Assert.NotNull(empty);
        Assert.True(empty.Value.IsEmpty);

        var content = await connection.ReadMessageAsync(1024, token);
        Assert.NotNull(content);
        Assert.Equal([.. "x"u8], content.Value.ToArray());

        Assert.Null(await connection.ReadMessageAsync(1024, token));
    }

    [Fact]
    public async Task BorrowedMemory_StaysValidOnlyUntilTheNextRead()
    {
        // Same-length messages keep the demonstration unambiguous: the
        // borrowed view keeps its length but observes the reused buffer.
        using var connection = new ScriptedMessageConnection(
        [
            [.. "AAAA"u8],
            [.. "BBBB"u8],
        ]);
        var token = TestContext.Current.CancellationToken;

        var borrowed = (await connection.ReadMessageAsync(1024, token)).Value;
        // A consumer that must retain the message copies it before reading on.
        var retained = borrowed.ToArray();

        await connection.ReadMessageAsync(1024, token);

        // The connection reuses its receive buffer (as real transports do);
        // the borrowed view now observes the newer message's bytes.
        Assert.Equal([.. "BBBB"u8], borrowed.ToArray());
        Assert.Equal([.. "AAAA"u8], retained);
    }

    [Fact]
    public async Task WriteMessageAsync_GathersSegmentsIntoOneMessage()
    {
        using var connection = new ScriptedMessageConnection([]);
        var token = TestContext.Current.CancellationToken;

        // A multi-segment input built the way the frame encoder builds wire
        // sequences: reusable segment nodes.
        var sequence = new FrameSequence();
        sequence.Add("head"u8.ToArray().AsMemory());
        sequence.Add("middle"u8.ToArray().AsMemory());
        sequence.Add("tail"u8.ToArray().AsMemory());

        await connection.WriteMessageAsync(sequence.Sequence, token);
        await connection.WriteMessageAsync(new ReadOnlySequence<byte>([.. "next"u8]), token);

        var messages = connection.Written;
        Assert.Equal(2, messages.Count);
        // One call is one message: segments gathered, boundaries preserved.
        Assert.Equal([.. "headmiddletail"u8], messages[0]);
        Assert.Equal([.. "next"u8], messages[1]);
    }

    [Fact]
    public async Task OversizedMessage_FaultsTheRead()
    {
        using var connection = new ScriptedMessageConnection([[.. "way too long"u8]]);
        var token = TestContext.Current.CancellationToken;

        // The bound is the transport's buffer limit; exceeding it surfaces as
        // the connection's own fault before any framing sees the bytes.
        await Assert.ThrowsAsync<ScriptedMessageConnection.MessageLimitException>(
            () => connection.ReadMessageAsync(4, token).AsTask());
    }

    [Fact]
    public async Task Abort_UnblocksAPendingRead_AndIsIdempotent()
    {
        // No script and no EOF: the read parks until the connection ends.
        using var connection = new ScriptedMessageConnection([]);
        var token = TestContext.Current.CancellationToken;

        var pending = connection.ReadMessageAsync(1024, token).AsTask();
        connection.Abort();
        connection.Abort();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }

    [Fact]
    public async Task ByteMembers_MayBeAFaithlessProjection()
    {
        // The byte view is optional fidelity: where a real view is
        // impossible (the empty-message/EOF ambiguity), throwing is part of
        // the contract, not a defect.
        using var connection = new ScriptedMessageConnection([]);
        var token = TestContext.Current.CancellationToken;
        Assert.Throws<NotSupportedException>(() => connection.ReadAsync(new byte[1], token));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => connection.WriteAsync(new ReadOnlyMemory<byte>(new byte[1]), token).AsTask());
    }

    /// <summary>
    /// A deterministic message-mode connection: serves the scripted messages
    /// in order from one reused receive buffer (making the borrowed lifetime
    /// observable); when the script is exhausted it either reports a clean
    /// EOF (<c>eofWhenExhausted</c>) or parks the read until
    /// <see cref="Abort"/>. Each write is recorded as one gathered message.
    /// The inherited byte members throw, exercising the documented
    /// faithful-projection opt-out.
    /// </summary>
    private sealed class ScriptedMessageConnection(byte[][] inbound, bool eofWhenExhausted = false)
        : IZMessageConnection
    {
        private readonly Queue<byte[]> _inbound = new(inbound);
        private readonly List<byte[]> _written = [];
        private readonly byte[] _receiveBuffer = new byte[1024];
        private readonly TaskCompletionSource _aborted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _disposed;

        public IReadOnlyList<byte[]> Written => _written;

        public ValueTask<ReadOnlyMemory<byte>?> ReadMessageAsync(long maxBytes, CancellationToken token = default)
        {
            if (_inbound.TryDequeue(out var message))
            {
                if (message.Length > maxBytes) throw new MessageLimitException(message.Length);

                // Copy into the reused buffer: the returned memory is
                // borrowed until the next read, exactly like a real
                // transport's receive buffer.
                message.CopyTo(_receiveBuffer, 0);
                return ValueTask.FromResult<ReadOnlyMemory<byte>?>(_receiveBuffer.AsMemory(0, message.Length));
            }

            if (eofWhenExhausted) return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);

            // No script and no EOF: park until abort ends the pending read.
            return new ValueTask<ReadOnlyMemory<byte>?>(ParkAsync(token));
        }

        public ValueTask WriteMessageAsync(ReadOnlySequence<byte> message, CancellationToken token = default)
        {
            // Completion consumes the entire input: gather the segments now.
            var gathered = new byte[checked((int)message.Length)];
            var position = 0;
            foreach (var segment in message)
            {
                segment.Span.CopyTo(gathered.AsSpan(position, segment.Length));
                position += segment.Length;
            }

            _written.Add(gathered);
            return ValueTask.CompletedTask;
        }

        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken token = default)
            => throw new NotSupportedException("the byte view cannot express message boundaries");

        public ValueTask WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken token = default)
            => throw new NotSupportedException("the byte view cannot express message boundaries");

        public ValueTask WriteAsync(ReadOnlySequence<byte> bytes, CancellationToken token = default)
            => throw new NotSupportedException("the byte view cannot express message boundaries");

        public void Abort() => _aborted.TrySetResult();

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

            _aborted.TrySetResult();
        }

        private async Task<ReadOnlyMemory<byte>?> ParkAsync(CancellationToken token)
        {
            await _aborted.Task.WaitAsync(token);
            throw new OperationCanceledException("connection aborted");
        }

        internal sealed class MessageLimitException(long length)
            : Exception($"message of {length} bytes exceeds the limit");
    }
}
