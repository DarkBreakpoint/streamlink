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

    [LoggerMessage(EventId = 6, Level = LogLevel.Information, Message = "Playlist loaded. TargetDuration: {TargetDuration}s, Sequence: {Sequence}, Segments: {Count}, EndList: {IsEndList}")]
    public static partial void PlaylistLoaded(this ILogger logger, double targetDuration, int sequence, int count, bool isEndList);

    [LoggerMessage(EventId = 7, Level = LogLevel.Debug, Message = "Segment {Num} downloaded ({Size} bytes) in {Duration}ms")]
    public static partial void SegmentDownloaded(this ILogger logger, int num, long size, double duration);

    [LoggerMessage(EventId = 8, Level = LogLevel.Warning, Message = "Stream stall detected after {Timeout}s")]
    public static partial void StreamStallDetected(this ILogger logger, double timeout);

    [LoggerMessage(EventId = 9, Level = LogLevel.Debug, Message = "New subscriber connected to broadcaster. Active: {ActiveCount}")]
    public static partial void BroadcasterSubscribed(this ILogger logger, int activeCount);

    [LoggerMessage(EventId = 10, Level = LogLevel.Debug, Message = "Subscriber disconnected from broadcaster. Active: {ActiveCount}")]
    public static partial void BroadcasterUnsubscribed(this ILogger logger, int activeCount);
}
