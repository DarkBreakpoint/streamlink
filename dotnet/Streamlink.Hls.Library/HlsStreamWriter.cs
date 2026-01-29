using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Diagnostics;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using Streamlink.Hls.Library.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Streamlink.Hls.Library;

public class HlsStreamWriter
{
    private readonly IHlsSession _session;
    private readonly Channel<HlsSegment> _inputChannel;
    private readonly HlsStreamBroadcaster _outputBroadcaster; // Replaced StreamBuffer
    private readonly ConcurrentDictionary<string, Task<byte[]>> _keyCache = new();
    private readonly HttpClient _httpClient;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly ILogger<HlsStreamWriter> _logger;

    public HlsStreamWriter(IHlsSession session, Channel<HlsSegment> inputChannel, HlsStreamBroadcaster outputBroadcaster, ILogger<HlsStreamWriter>? logger = null)
    {
        _session = session;
        _inputChannel = inputChannel;
        _outputBroadcaster = outputBroadcaster;
        _httpClient = session.HttpClient;
        _logger = logger ?? NullLogger<HlsStreamWriter>.Instance;

        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = Math.Max(1, session.Options.StreamSegmentAttempts),
                Delay = TimeSpan.FromSeconds(1),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            })
            .AddTimeout(TimeSpan.FromSeconds(session.Options.StreamSegmentTimeout))
            .Build();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        int threads = Math.Max(1, _session.Options.StreamSegmentThreads);

        var orderChannel = Channel.CreateBounded<ValueTask<Stream?>>(new BoundedChannelOptions(threads)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        var downloadTask = ProduceDownloadsSlidingWindowAsync(orderChannel.Writer, cancellationToken);
        var writeTask = ConsumeAndWriteAsync(orderChannel.Reader, cancellationToken);

        await Task.WhenAll(downloadTask, writeTask);
    }

    private async Task ProduceDownloadsSlidingWindowAsync(ChannelWriter<ValueTask<Stream?>> writer, CancellationToken ct)
    {
        try
        {
            Map? lastMap = null;
            await foreach (var segment in _inputChannel.Reader.ReadAllAsync(ct))
            {
                if (segment.Map != null && segment.Map != lastMap)
                {
                    var mapTask = StartDownloadMap(segment.Map, segment, ct);
                    await writer.WriteAsync(new ValueTask<Stream?>(mapTask), ct);
                    lastMap = segment.Map;
                }

                var task = StartDownloadSegment(segment, ct);
                await writer.WriteAsync(new ValueTask<Stream?>(task), ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
             _logger.DownloaderFailed(ex.Message);
        }
        finally
        {
            writer.Complete();
        }
    }

    private Task<Stream?> StartDownloadMap(Map map, HlsSegment context, CancellationToken ct)
    {
        return DownloadMapAsync(map, context, ct).AsTask();
    }

    private Task<Stream?> StartDownloadSegment(HlsSegment segment, CancellationToken ct)
    {
        return DownloadSegmentAsync(segment, ct).AsTask();
    }

    private async Task ConsumeAndWriteAsync(ChannelReader<ValueTask<Stream?>> reader, CancellationToken ct)
    {
        try
        {
            byte[] buffer = new byte[8192];

            await foreach (var task in reader.ReadAllAsync(ct))
            {
                using var stream = await task;
                if (stream != null)
                {
                    int read;
                    while ((read = await stream.ReadAsync(buffer, ct)) > 0)
                    {
                        // Copy data to broadcast hub
                        // Need to create a copy because buffer is reused
                        await _outputBroadcaster.WriteAsync(new ReadOnlyMemory<byte>(buffer.AsSpan(0, read).ToArray()), ct);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _outputBroadcaster.Complete(ex);
            return;
        }

        _outputBroadcaster.Complete();
    }

    private async ValueTask<Stream?> DownloadMapAsync(Map map, HlsSegment context, CancellationToken ct)
    {
        try
        {
            return await _resiliencePipeline.ExecuteAsync(async (token) =>
            {
                 return await FetchAndDecryptAsync(map.Uri, map.Key, map.ByteRange, context.Num, token);
            }, ct);
        }
        catch (Exception ex)
        {
             _logger.MapDownloadFailed(map.Uri, ex.Message);
             return null;
        }
    }

    private async ValueTask<Stream?> DownloadSegmentAsync(HlsSegment segment, CancellationToken ct)
    {
        if (ShouldFilter(segment)) return null;

        var sw = Stopwatch.StartNew();
        try
        {
            var result = await _resiliencePipeline.ExecuteAsync(async (token) =>
            {
                return await FetchAndDecryptAsync(segment.Uri, segment.Key, segment.ByteRange, segment.Num, token);
            }, ct);

            sw.Stop();
            HlsMetrics.SegmentDownloadDuration.Record(sw.Elapsed.TotalMilliseconds);
            return result;
        }
        catch (Exception ex)
        {
            HlsMetrics.SegmentDownloadErrors.Add(1);
            _logger.SegmentDownloadFailed(segment.Num, segment.Uri, ex.Message);
            return null;
        }
    }

    private bool ShouldFilter(HlsSegment segment)
    {
        if (_session.Options.SegmentIgnoreNames.Count > 0)
        {
             foreach (var ignore in _session.Options.SegmentIgnoreNames)
             {
                 if (segment.Uri.Contains(ignore)) return true;
             }
        }
        return false;
    }

    private async ValueTask<Stream> FetchAndDecryptAsync(string uri, Key? key, ByteRange? byteRange, int sequenceNum, CancellationToken ct)
    {
        byte[]? keyData = null;
        byte[]? iv = null;

        if (key != null && key.Method != "NONE")
        {
             if (key.Method != "AES-128") throw new StreamError($"Unsupported encryption method: {key.Method}");

             string keyUri = key.Uri ?? "";
             if (!string.IsNullOrEmpty(_session.Options.SegmentKeyUriOverride))
             {
                 keyUri = _session.Options.SegmentKeyUriOverride;
             }

             if (string.IsNullOrEmpty(keyUri)) throw new StreamError("Missing Key URI");

             keyData = await GetKeyAsync(keyUri, ct);

             if (key.Iv != null) iv = key.Iv;
             else iv = AesUtil.CreateIv(sequenceNum);
        }

        var req = new HttpRequestMessage(HttpMethod.Get, uri);
        if (byteRange.HasValue)
        {
             if (byteRange.Value.Offset.HasValue)
             {
                 long from = byteRange.Value.Offset.Value;
                 long to = from + byteRange.Value.Range - 1;
                 req.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(from, to);
             }
        }

        var response = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        long? contentLength = response.Content.Headers.ContentLength;
        if (contentLength.HasValue)
        {
            HlsMetrics.BytesDownloaded.Add(contentLength.Value);
        }

        var networkStream = await response.Content.ReadAsStreamAsync(ct);

        var stallStream = new StallDetectingStream(networkStream, TimeSpan.FromSeconds(_session.Options.StreamStallTimeout));

        Stream resultStream = stallStream;
        if (keyData != null && iv != null)
        {
             resultStream = AesUtil.CreateDecryptingStream(stallStream, keyData, iv);
        }

        return resultStream;
    }

    private Task<byte[]> GetKeyAsync(string uri, CancellationToken ct)
    {
        return _keyCache.GetOrAdd(uri, async u =>
        {
            var req = new HttpRequestMessage(HttpMethod.Get, u);
            using var res = await _httpClient.SendAsync(req, ct);
            res.EnsureSuccessStatusCode();
            return await res.Content.ReadAsByteArrayAsync(ct);
        });
    }
}
