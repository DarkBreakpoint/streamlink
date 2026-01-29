using System.Diagnostics.Metrics;

namespace Streamlink.Hls.Library;

public static class HlsMetrics
{
    public const string MeterName = "Streamlink.Hls.Library";
    private static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> BytesDownloaded = Meter.CreateCounter<long>("hls.bytes_downloaded", "B", "Total bytes downloaded");
    public static readonly Histogram<double> SegmentDownloadDuration = Meter.CreateHistogram<double>("hls.segment_download_duration", "ms", "Duration of segment downloads");
    public static readonly Counter<long> SegmentDownloadErrors = Meter.CreateCounter<long>("hls.segment_download_errors", "1", "Count of failed segment downloads");
    public static readonly ObservableGauge<int> QueuedSegments = Meter.CreateObservableGauge<int>("hls.queued_segments", () => 0, "1", "Number of segments queued for download");

    // Note: ObservableGauge for QueuedSegments needs a way to access the queue length dynamically.
    // For simplicity, we might just track it manually via a Counter if accessing internal state is hard,
    // or register a callback if we refactor.
    // Let's stick to Counters/Histograms for now unless we link it to Writer state.
}
