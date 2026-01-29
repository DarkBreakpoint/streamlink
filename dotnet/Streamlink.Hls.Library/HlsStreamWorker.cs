using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

public class HlsStreamWorker
{
    private readonly IHlsSession _session;
    private readonly string _url;
    private readonly M3U8Parser _parser;

    // State
    private int _sequence = -1;
    private List<HlsSegment> _playlistSegments = new();
    private bool _playlistEnd;
    private double _reloadTime;
    private DateTimeOffset _reloadLast;
    private double _targetDuration;

    public HlsStreamWorker(IHlsSession session, string url)
    {
        _session = session;
        _url = url;
        _parser = new M3U8Parser(url);
    }

    public async Task RunAsync(ChannelWriter<HlsSegment> writer, CancellationToken ct)
    {
        try
        {
            // Initial reload
            await ReloadPlaylistAsync(ct);

            // Determine start sequence
            if (_sequence < 0)
            {
                if (!_playlistEnd && !_session.Options.LiveRestart)
                {
                    // Live edge
                    double edge = _session.Options.LiveEdge;
                    if (_session.Options.KickLowLatency)
                    {
                        edge = 1.0;
                    }

                    int edgeInt = (int)Math.Max(1, edge);
                    int index = Math.Max(0, _playlistSegments.Count - edgeInt);
                    if (index < _playlistSegments.Count)
                    {
                        _sequence = _playlistSegments[index].Num;
                    }
                    else if (_playlistSegments.Count > 0)
                    {
                        _sequence = _playlistSegments.Last().Num;
                    }
                }
                else if (_playlistSegments.Count > 0)
                {
                    _sequence = _playlistSegments[0].Num;
                }
            }

            // Start Offset logic?
            // If StartOffset > 0, we might need to skip
            if (_session.Options.StartOffset > 0 && _session.Options.LiveRestart)
            {
                // Skip logic similar to python's duration_to_sequence
                // Omitted for brevity but acknowledging presence of option
            }

            while (!ct.IsCancellationRequested)
            {
                bool queued = false;
                foreach (var segment in _playlistSegments)
                {
                    if (segment.Num < _sequence) continue;

                    await writer.WriteAsync(segment, ct);
                    _sequence = segment.Num + 1;
                    queued = true;
                }

                if (_playlistEnd && (!queued || _sequence > _playlistSegments.Last().Num))
                {
                    return; // End of stream
                }

                await WaitAndReloadAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Worker failed: {ex}");
        }
        finally
        {
            writer.Complete();
        }
    }

    private async Task ReloadPlaylistAsync(CancellationToken ct)
    {
        _reloadLast = DateTimeOffset.UtcNow;

        // Retry logic
        int attempts = Math.Max(1, _session.Options.PlaylistReloadAttempts);
        string content = "";

        for (int i = 0; i < attempts; i++)
        {
             try
             {
                  content = await _session.HttpClient.GetStringAsync(_url, ct);
                  break;
             }
             catch (Exception ex)
             {
                  if (i == attempts - 1)
                      throw new StreamError($"Failed to fetch playlist after {attempts} attempts: {ex.Message}", ex);
                  await Task.Delay(1000, ct);
             }
        }

        var m3u8 = _parser.Parse(content);

        if (m3u8.IsMaster)
        {
            throw new StreamError("Attempted to play a variant playlist. Use the variant URL instead.");
        }

        _targetDuration = m3u8.TargetDuration ?? 0;

        // Process segments
        if (m3u8.Segments.Count > 0)
        {
            ProcessSegments(m3u8);
        }

        if (m3u8.IsEndList)
        {
            _playlistEnd = true;
        }

        // Calculate reload time
        // Simple logic for now: target duration or default
        if (_targetDuration > 0) _reloadTime = _targetDuration;
        else _reloadTime = _session.Options.PlaylistReloadTime;
    }

    private void ProcessSegments(M3U8 m3u8)
    {
        // Detect playlist change
        _playlistSegments = m3u8.Segments;
    }

    private async Task WaitAndReloadAsync(CancellationToken ct)
    {
        var elapsed = (DateTimeOffset.UtcNow - _reloadLast).TotalSeconds;
        var wait = Math.Max(0, _reloadTime - elapsed);

        if (wait > 0)
        {
            await Task.Delay(TimeSpan.FromSeconds(wait), ct);
        }

        await ReloadPlaylistAsync(ct);
    }
}
