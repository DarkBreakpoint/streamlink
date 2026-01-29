using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

public class HlsStreamWriter
{
    private readonly IHlsSession _session;
    private readonly Channel<HlsSegment> _inputChannel;
    private readonly StreamBuffer _outputBuffer;
    private readonly ConcurrentDictionary<string, Task<byte[]>> _keyCache = new();
    private readonly HttpClient _httpClient;

    public HlsStreamWriter(IHlsSession session, Channel<HlsSegment> inputChannel, StreamBuffer outputBuffer)
    {
        _session = session;
        _inputChannel = inputChannel;
        _outputBuffer = outputBuffer;
        _httpClient = session.HttpClient;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // Channel to preserve order of segments/maps to be written
        // Use StreamSegmentThreads to control parallelism (capacity)
        int threads = Math.Max(1, _session.Options.StreamSegmentThreads);

        var orderChannel = Channel.CreateBounded<Task<Stream?>>(new BoundedChannelOptions(threads)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        // Start Producer (Downloader)
        var downloadTask = ProduceDownloadsAsync(orderChannel.Writer, cancellationToken);

        // Start Consumer (Writer)
        var writeTask = ConsumeAndWriteAsync(orderChannel.Reader, cancellationToken);

        await Task.WhenAll(downloadTask, writeTask);
    }

    private async Task ProduceDownloadsAsync(ChannelWriter<Task<Stream?>> writer, CancellationToken ct)
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

    private async Task ConsumeAndWriteAsync(ChannelReader<Task<Stream?>> reader, CancellationToken ct)
    {
        try
        {
            // Get output stream wrapper for PipeWriter
            using var outputStream = _outputBuffer.Writer.AsStream(true); // leaveOpen=true

            await foreach (var task in reader.ReadAllAsync(ct))
            {
                using var stream = await task;
                if (stream != null)
                {
                    await stream.CopyToAsync(outputStream, ct);
                    await _outputBuffer.Writer.FlushAsync(ct); // Ensure flushed
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

    private async Task<Stream?> DownloadMapAsync(Map map, HlsSegment context, CancellationToken ct)
    {
        return await RetryAsync(async (token) =>
        {
             return await FetchAndDecryptAsync(map.Uri, map.Key, map.ByteRange, context.Num, token);
        }, ct, "Map download");
    }

    private async Task<Stream?> DownloadSegmentAsync(HlsSegment segment, CancellationToken ct)
    {
        if (ShouldFilter(segment)) return null;

        return await RetryAsync(async (token) =>
        {
            return await FetchAndDecryptAsync(segment.Uri, segment.Key, segment.ByteRange, segment.Num, token);
        }, ct, $"Segment {segment.Num}");
    }

    private async Task<T?> RetryAsync<T>(Func<CancellationToken, Task<T>> func, CancellationToken ct, string context)
    {
        int attempts = Math.Max(1, _session.Options.StreamSegmentAttempts);
        for (int i = 0; i < attempts; i++)
        {
            try
            {
                // Create a timeout token for the attempt
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_session.Options.StreamSegmentTimeout));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

                return await func(linkedCts.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (i == attempts - 1)
                {
                    Console.Error.WriteLine($"Failed {context} after {attempts} attempts: {ex.Message}");
                    return default;
                }
                // Backoff?
                await Task.Delay(1000, ct); // Simple delay
            }
        }
        return default;
    }

    private bool ShouldFilter(HlsSegment segment)
    {
        // ignore names
        if (_session.Options.SegmentIgnoreNames.Count > 0)
        {
             // Simple contains check or regex?
             // Python implementation uses regex.
             // Here we have list of strings. Assuming simple substring or use Regex if passed as such.
             foreach (var ignore in _session.Options.SegmentIgnoreNames)
             {
                 if (segment.Uri.Contains(ignore)) return true;
             }
        }
        return false;
    }

    private async Task<Stream> FetchAndDecryptAsync(string uri, Key? key, ByteRange? byteRange, int sequenceNum, CancellationToken ct)
    {
        // 1. Fetch Key if needed
        byte[]? keyData = null;
        byte[]? iv = null;

        if (key != null && key.Method != "NONE")
        {
             if (key.Method != "AES-128") throw new StreamError($"Unsupported encryption method: {key.Method}");

             string keyUri = key.Uri ?? "";
             if (!string.IsNullOrEmpty(_session.Options.SegmentKeyUriOverride))
             {
                 // Should format override with keyUri parts?
                 // Simple override for now as per minimal requirement, or implementing full formatter?
                 // Python does formatting.
                 keyUri = _session.Options.SegmentKeyUriOverride;
             }

             if (string.IsNullOrEmpty(keyUri)) throw new StreamError("Missing Key URI");

             keyData = await GetKeyAsync(keyUri, ct);

             if (key.Iv != null) iv = key.Iv;
             else iv = AesUtil.CreateIv(sequenceNum);
        }

        // 2. Fetch Data
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
        var networkStream = await response.Content.ReadAsStreamAsync(ct);

        // 3. Decrypt
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
