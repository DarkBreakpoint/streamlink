using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

public class HlsStream : IDisposable
{
    private readonly IHlsSession _session;
    private readonly string _url;
    private readonly ILogger<HlsStream> _logger;
    private readonly ILoggerFactory _loggerFactory;

    private Channel<HlsSegment>? _segmentChannel;
    private HlsStreamBroadcaster? _broadcaster;
    private HlsStreamWorker? _worker;
    private HlsStreamWriter? _writer;
    private CancellationTokenSource? _lifecycleCts;
    private Task? _workerTask;
    private Task? _writerTask;
    private bool _started;
    private readonly object _lock = new();

    public HlsStream(IHlsSession session, string url, ILoggerFactory? loggerFactory = null)
    {
        _session = session;
        _url = url;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<HlsStream>();
    }

    public async Task<Stream> OpenAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (!_started)
            {
                StartProcessing();
                _started = true;
            }
        }

        // Subscribe to broadcaster
        return _broadcaster!.Subscribe(cancellationToken);
    }

    private void StartProcessing()
    {
        _lifecycleCts = new CancellationTokenSource();

        _segmentChannel = Channel.CreateBounded<HlsSegment>(new BoundedChannelOptions(100)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        _broadcaster = new HlsStreamBroadcaster();

        _worker = new HlsStreamWorker(_session, _url, _loggerFactory.CreateLogger<HlsStreamWorker>());
        _writer = new HlsStreamWriter(_session, _segmentChannel, _broadcaster, _loggerFactory.CreateLogger<HlsStreamWriter>());

        var token = _lifecycleCts.Token;

        _workerTask = Task.Run(async () =>
        {
            try
            {
                await _worker.RunAsync(_segmentChannel.Writer, token);
            }
            catch (Exception ex)
            {
                _segmentChannel.Writer.Complete(ex);
            }
        }, token);

        _writerTask = Task.Run(async () =>
        {
             try
             {
                 await _writer.RunAsync(token);
             }
             catch (Exception ex)
             {
                 _broadcaster.Complete(ex);
             }
        }, token);
    }

    public void Dispose()
    {
        _lifecycleCts?.Cancel();
        _lifecycleCts?.Dispose();
        _broadcaster?.Dispose();
    }
}
