using System.Net.Http;

namespace Streamlink.Hls.Library;

public class HlsOptions
{
    public bool LiveRestart { get; set; }
    public double LiveEdge { get; set; } = 3.0; // Default segments from edge
    public double StartOffset { get; set; }
    public double Duration { get; set; }
    public int PlaylistReloadAttempts { get; set; } = 3;
    public double PlaylistReloadTime { get; set; } = 6.0;

    // Segment handling
    public string? SegmentKeyUriOverride { get; set; }
    public bool SegmentStreamData { get; set; }
    public List<string> SegmentIgnoreNames { get; set; } = new();

    public int StreamTimeout { get; set; } = 10;
    public int Retries { get; set; } = 3;

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
        HttpClient = httpClient ?? new HttpClient();
    }
}
