using System;
using System.Net;
using System.Net.Http;
using System.Collections.Generic;

namespace Streamlink.Hls.Library;

public class HlsOptions
{
    // HLS Specific
    public bool LiveRestart { get; set; }
    public double LiveEdge { get; set; } = 3.0;
    public double StartOffset { get; set; }
    public double Duration { get; set; }
    public int PlaylistReloadAttempts { get; set; } = 3;
    public double PlaylistReloadTime { get; set; } = 6.0;
    public double HlsSegmentQueueThreshold { get; set; } = 3.0; // Default factor?
    public List<string> HlsAudioSelect { get; set; } = new();

    // Segment handling
    public string? SegmentKeyUriOverride { get; set; }
    public bool SegmentStreamData { get; set; }
    public List<string> SegmentIgnoreNames { get; set; } = new();

    // Stream General / Segmented
    public int StreamSegmentAttempts { get; set; } = 3;
    public int StreamSegmentThreads { get; set; } = 1;
    public double StreamSegmentTimeout { get; set; } = 10.0;
    public double StreamTimeout { get; set; } = 60.0; // Connection timeout
    public double StreamStallTimeout { get; set; } = 5.0; // Read stall timeout

    public double? StreamSegmentedDuration { get; set; }
    public double? StreamSegmentedQueueDeadline { get; set; }

    public bool KickLowLatency { get; set; } // Reduce live edge for low latency?

    // FFMPEG (Future use)
    public string? FfmpegFfmpeg { get; set; }
    public string? FfmpegVideoTranscode { get; set; } = "copy";
    public string? FfmpegAudioTranscode { get; set; } = "copy";
}

public interface IHlsSession
{
    HlsOptions Options { get; }
    HttpClient HttpClient { get; }
}

public class HlsSession : IHlsSession
{
    public HlsOptions Options { get; }
    public HttpClient HttpClient { get; }

    public HlsSession(HlsOptions? options = null, HttpClient? httpClient = null)
    {
        Options = options ?? new HlsOptions();

        if (httpClient != null)
        {
            HttpClient = httpClient;
        }
        else
        {
            // Custom SocketsHttpHandler for HTTP/3 and tuning
            var handler = new SocketsHttpHandler
            {
                ConnectTimeout = TimeSpan.FromSeconds(10), // Fast failover
                PooledConnectionLifetime = TimeSpan.FromMinutes(2), // DNS refresh
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(5)
            };

            HttpClient = new HttpClient(handler)
            {
                DefaultRequestVersion = HttpVersion.Version30,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower
            };
        }

        HttpClient.Timeout = TimeSpan.FromSeconds(Options.StreamTimeout);
    }
}
