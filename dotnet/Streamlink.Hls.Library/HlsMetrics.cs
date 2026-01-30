using System.Diagnostics.Metrics;

namespace Streamlink.Hls.Library;

public static class HlsMetrics
{
    public const string MeterName = "Streamlink.Hls.Library";
    private static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> BytesDownloaded = Meter.CreateCounter<long>("hls.bytes_downloaded", "B", "Total bytes downloaded");
    public static readonly Histogram<double> SegmentDownloadDuration = Meter.CreateHistogram<double>("hls.segment_download_duration", "ms", "Duration of segment downloads");
    public static readonly Counter<long> SegmentDownloadErrors = Meter.CreateCounter<long>("hls.segment_download_errors", "1", "Count of failed segment downloads");

    public static readonly Counter<long> PlaylistRefreshes = Meter.CreateCounter<long>("hls.playlist_refreshes", "1", "Count of successful playlist reloads");
    public static readonly Histogram<double> PlaylistRefreshDuration = Meter.CreateHistogram<double>("hls.playlist_refresh_duration", "ms", "Time taken to reload playlist");

    public static readonly UpDownCounter<int> ActiveSubscribers = Meter.CreateUpDownCounter<int>("hls.active_subscribers", "1", "Number of active stream readers");
    public static readonly Counter<long> StallEvents = Meter.CreateCounter<long>("hls.stall_events", "1", "Count of stream stall events detected");

    public static readonly Counter<long> SubscriberDroppedChunks = Meter.CreateCounter<long>("hls.subscriber_dropped_chunks", "1", "Number of chunks dropped due to slow readers");
    public static readonly Histogram<int> SubscriberBufferCount = Meter.CreateHistogram<int>("hls.subscriber_buffer_count", "1", "Number of chunks currently buffered per subscriber");
}
