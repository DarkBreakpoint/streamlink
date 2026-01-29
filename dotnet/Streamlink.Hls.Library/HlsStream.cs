using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

public class HlsStream
{
    private readonly IHlsSession _session;
    private readonly string _url;

    public HlsStream(IHlsSession session, string url)
    {
        _session = session;
        _url = url;
    }

    public async Task<Stream> OpenAsync(CancellationToken cancellationToken = default)
    {
        // 1. Setup channels and buffer
        var segmentChannel = Channel.CreateBounded<HlsSegment>(new BoundedChannelOptions(100)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        var buffer = new StreamBuffer();

        // 2. Create Worker and Writer
        var worker = new HlsStreamWorker(_session, _url);
        var writer = new HlsStreamWriter(_session, segmentChannel, buffer);

        // 3. Start background tasks
        // We need a CancellationTokenSource linked to the stream lifetime
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Fire and forget tasks (monitored via completion)
        _ = Task.Run(async () =>
        {
            try
            {
                await worker.RunAsync(segmentChannel.Writer, cts.Token);
            }
            catch (Exception ex)
            {
                // If worker fails, we should probably fail the writer -> buffer
                // The writer will complete when channel completes.
                // But if worker crashes with exception, channel completes with exception?
                segmentChannel.Writer.Complete(ex);
            }
        }, cts.Token);

        _ = Task.Run(async () =>
        {
             try
             {
                 await writer.RunAsync(cts.Token);
             }
             catch (Exception ex)
             {
                 buffer.CompleteWriter(ex);
             }
        }, cts.Token);

        // 4. Return stream
        // When the stream is disposed, we should cancel the CTS.
        var stream = buffer.Reader.AsStream(true); // leaveOpen=true because we handle disposal? No, AsStream returns a wrapper.
        // We need to wrap this stream to cancel CTS on Dispose.

        return new HlsReadOnlyStream(stream, cts);
    }

    private class HlsReadOnlyStream : Stream
    {
        private readonly Stream _inner;
        private readonly CancellationTokenSource _cts;

        public HlsReadOnlyStream(Stream inner, CancellationTokenSource cts)
        {
            _inner = inner;
            _cts = cts;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }

        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.ReadAsync(buffer, offset, count, cancellationToken);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cts.Cancel();
                _cts.Dispose();
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
