using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Streamlink.Hls.Library;

public class HlsStreamBroadcaster : IDisposable
{
    private readonly ConcurrentDictionary<Guid, Channel<ReadOnlyMemory<byte>>> _subscribers = new();
    private readonly int _bufferCapacity;
    private bool _completed;
    private Exception? _error;
    private readonly ILogger<HlsStreamBroadcaster> _logger;

    public HlsStreamBroadcaster(int bufferCapacity = 100, ILogger<HlsStreamBroadcaster>? logger = null)
    {
        _bufferCapacity = bufferCapacity;
        _logger = logger ?? NullLogger<HlsStreamBroadcaster>.Instance;
    }

    public Stream Subscribe(CancellationToken ct)
    {
        var options = new BoundedChannelOptions(_bufferCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = true
        };

        var channel = Channel.CreateBounded<ReadOnlyMemory<byte>>(options);
        var id = Guid.NewGuid();

        _subscribers.TryAdd(id, channel);
        HlsMetrics.ActiveSubscribers.Add(1);
        _logger.BroadcasterSubscribed(_subscribers.Count);

        return new ChannelStream(channel.Reader, () => Unsubscribe(id));
    }

    private void Unsubscribe(Guid id)
    {
        if (_subscribers.TryRemove(id, out var channel))
        {
            channel.Writer.TryComplete();
            HlsMetrics.ActiveSubscribers.Add(-1);
            _logger.BroadcasterUnsubscribed(_subscribers.Count);
        }
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        if (_completed) return;

        foreach (var sub in _subscribers)
        {
            int count = sub.Value.Reader.Count;
            HlsMetrics.SubscriberBufferCount.Record(count);

            if (count >= _bufferCapacity)
            {
                HlsMetrics.SubscriberDroppedChunks.Add(1);
            }
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
                        return totalRead;
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
