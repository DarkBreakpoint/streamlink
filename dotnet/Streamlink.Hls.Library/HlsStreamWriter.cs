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

namespace Streamlink.Hls.Library;

public class HlsStreamWriter
{
    private readonly IHlsSession _session;
    private readonly Channel<HlsSegment> _inputChannel;
    private readonly StreamBuffer _outputBuffer;
    private readonly ConcurrentDictionary<string, Task<byte[]>> _keyCache = new();
    private readonly HttpClient _httpClient;
    private readonly ResiliencePipeline _resiliencePipeline;

    public HlsStreamWriter(IHlsSession session, Channel<HlsSegment> inputChannel, StreamBuffer outputBuffer)
    {
        _session = session;
        _inputChannel = inputChannel;
        _outputBuffer = outputBuffer;
        _httpClient = session.HttpClient;

        // Build Resilience Pipeline
        // Combines Retry and Timeout
        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = Math.Max(1, session.Options.StreamSegmentAttempts),
                Delay = TimeSpan.FromSeconds(1), // Initial delay
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

        var downloadTask = ProduceDownloadsAsync(orderChannel.Writer, cancellationToken);
        var writeTask = ConsumeAndWriteAsync(orderChannel.Reader, cancellationToken);

        await Task.WhenAll(downloadTask, writeTask);
    }

    private async Task ProduceDownloadsAsync(ChannelWriter<ValueTask<Stream?>> writer, CancellationToken ct)
    {
        try
        {
            Map? lastMap = null;

            await foreach (var segment in _inputChannel.Reader.ReadAllAsync(ct))
            {
                if (segment.Map != null && segment.Map != lastMap)
                {
                    await writer.WriteAsync(DownloadMapAsync(segment.Map, segment, ct), ct);
                    lastMap = segment.Map;
                }

                await writer.WriteAsync(DownloadSegmentAsync(segment, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
             Console.Error.WriteLine($"Downloader failed: {ex}");
        }
        finally
        {
            writer.Complete();
        }
    }

    private async Task ConsumeAndWriteAsync(ChannelReader<ValueTask<Stream?>> reader, CancellationToken ct)
    {
        try
        {
            using var outputStream = _outputBuffer.Writer.AsStream(true);

            await foreach (var task in reader.ReadAllAsync(ct))
            {
                using var stream = await task;
                if (stream != null)
                {
                    await stream.CopyToAsync(outputStream, ct);
                    await _outputBuffer.Writer.FlushAsync(ct);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _outputBuffer.CompleteWriter(ex);
            return;
        }

        _outputBuffer.CompleteWriter();
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
             Console.Error.WriteLine($"Failed to download map {map.Uri}: {ex.Message}");
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
        catch
        {
            HlsMetrics.SegmentDownloadErrors.Add(1);
            Console.Error.WriteLine($"Failed to download segment {segment.Num}: {segment.Uri}");
            return null; // Return null on failure after retries exhausted
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

        if (keyData != null && iv != null)
        {
             return AesUtil.CreateDecryptingStream(networkStream, keyData, iv);
        }

        return networkStream;
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
