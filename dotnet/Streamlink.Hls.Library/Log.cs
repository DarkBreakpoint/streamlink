using System;
using Microsoft.Extensions.Logging;

namespace Streamlink.Hls.Library;

public static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "Downloader failed: {Message}")]
    public static partial void DownloaderFailed(this ILogger logger, string message);

    [LoggerMessage(EventId = 2, Level = LogLevel.Error, Message = "Failed to download map {Uri}: {Message}")]
    public static partial void MapDownloadFailed(this ILogger logger, string uri, string message);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "Failed to download segment {Num} ({Uri}): {Message}")]
    public static partial void SegmentDownloadFailed(this ILogger logger, int num, string uri, string message);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Failed {Context} after {Attempts} attempts: {Message}")]
    public static partial void RetryFailed(this ILogger logger, string context, int attempts, string message);

    [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Worker failed: {Message}")]
    public static partial void WorkerFailed(this ILogger logger, string message);
}
