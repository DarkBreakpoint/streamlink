using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Streamlink.Hls.Library;

public class HlsStreamBroadcaster : IDisposable
{
    private readonly ConcurrentDictionary<Guid, Channel<ReadOnlyMemory<byte>>> _subscribers = new();
    private readonly int _bufferCapacity;
    private bool _completed;
    private Exception? _error;

    public HlsStreamBroadcaster(int bufferCapacity = 100)
    {
        _bufferCapacity = bufferCapacity;
    }

    public Stream Subscribe(CancellationToken ct)
    {
        var options = new BoundedChannelOptions(_bufferCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest, // Ring buffer behavior: drop old for slow readers
            SingleReader = true,
            SingleWriter = true
        };

        var channel = Channel.CreateBounded<ReadOnlyMemory<byte>>(options);
        var id = Guid.NewGuid();

        _subscribers.TryAdd(id, channel);

        // Return a Stream wrapper
        return new ChannelStream(channel.Reader, () => Unsubscribe(id));
    }

    private void Unsubscribe(Guid id)
    {
        if (_subscribers.TryRemove(id, out var channel))
        {
            channel.Writer.TryComplete();
        }
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        if (_completed) return;

        foreach (var sub in _subscribers)
        {
            // DropOldest handles the "ring buffer" overwriting logic automatically
            // If the reader is slow, this TryWrite (or WriteAsync) will drop the oldest item if full.
            // Note: WriteAsync with DropOldest completes synchronously usually.
            await sub.Value.Writer.WriteAsync(data, ct);
        }
    }

    public void Complete(Exception? error = null)
    {
        _completed = true;
        _error = error;
        foreach (var sub in _subscribers)
        {
            sub.Value.Writer.TryComplete(error);
        }
    }

    public void Dispose()
    {
        Complete(new ObjectDisposedException(nameof(HlsStreamBroadcaster)));
    }

    private class ChannelStream : Stream
    {
        private readonly ChannelReader<ReadOnlyMemory<byte>> _reader;
        private readonly Action _onDispose;
        private ReadOnlyMemory<byte> _currentBlock;
        private int _currentOffset;

        public ChannelStream(ChannelReader<ReadOnlyMemory<byte>> reader, Action onDispose)
        {
            _reader = reader;
            _onDispose = onDispose;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            // Sync-over-async avoidance: This is a legacy path.
            // Ideally callers use ReadAsync.
            // We'll implement a blocking read but it's not ideal.
            return ReadAsync(buffer, offset, count).AsTask().GetAwaiter().GetResult();
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return await ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int totalRead = 0;

            while (buffer.Length > 0)
            {
                if (_currentBlock.Length == 0)
                {
                    if (!await _reader.WaitToReadAsync(cancellationToken))
                    {
                        return totalRead; // End of stream
                    }

                    if (!_reader.TryRead(out _currentBlock))
                    {
                        continue;
                    }
                    _currentOffset = 0;
                }

                int available = _currentBlock.Length - _currentOffset;
                int toCopy = Math.Min(available, buffer.Length);

                _currentBlock.Slice(_currentOffset, toCopy).CopyTo(buffer);

                _currentOffset += toCopy;
                buffer = buffer.Slice(toCopy);
                totalRead += toCopy;

                if (_currentOffset >= _currentBlock.Length)
                {
                    _currentBlock = default;
                    // If we read something, return immediately to allow streaming,
                    // or keep reading if we want to fill buffer?
                    // Typically ReadAsync returns whatever is available.
                    return totalRead;
                }
            }

            return totalRead;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                _onDispose();
            }
        }
    }
}
