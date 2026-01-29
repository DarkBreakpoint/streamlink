using System;
using System.Collections.Generic;

namespace Streamlink.Hls.Library.Models;

public readonly record struct Resolution(int Width, int Height);

public readonly record struct ExtInf(double Duration, string? Title);

public readonly record struct ByteRange(long Range, long? Offset);

public readonly record struct Start(double TimeOffset, bool Precise);

public record Key(
    string Method,
    string? Uri,
    byte[]? Iv,
    string? KeyFormat,
    string? KeyFormatVersions
);

public record Map(
    string Uri,
    Key? Key,
    ByteRange? ByteRange
);

public record DateRange(
    string? Id,
    string? ClassName,
    DateTimeOffset? StartDate,
    DateTimeOffset? EndDate,
    TimeSpan? Duration,
    TimeSpan? PlannedDuration,
    bool EndOnNext,
    Dictionary<string, string> Attributes
);

public record Media(
    string? Uri,
    string Type,
    string GroupId,
    string? Language,
    string Name,
    bool Default,
    bool Autoselect,
    bool Forced,
    string? Characteristics
)
{
    // ParsedLanguage omitted for now, can be added if needed
}

public partial record StreamInfo(
    int Bandwidth,
    string? ProgramId,
    List<string> Codecs,
    Resolution? Resolution,
    string? Audio,
    string? Video,
    string? Subtitles
);

public partial record IFrameStreamInfo(
    int Bandwidth,
    string? ProgramId,
    List<string> Codecs,
    Resolution? Resolution,
    string? Video
);

// Discriminated union or just base class for StreamInfo?
// In Python it's a Union. In C# we can use a wrapper or just use object.
// Or we can have a common base/interface if they share fields.
// Bandwidth, ProgramId, Codecs, Resolution, Video are shared.

public interface IStreamInfo
{
    int Bandwidth { get; }
    string? ProgramId { get; }
    List<string> Codecs { get; }
    Resolution? Resolution { get; }
    string? Video { get; }
}

public partial record StreamInfo : IStreamInfo;
public partial record IFrameStreamInfo : IStreamInfo;

public record HlsPlaylist(
    string Uri,
    IStreamInfo StreamInfo,
    List<Media> Media,
    bool IsIFrame
);

public record Segment(
    int Num,
    bool Init,
    bool Discontinuity,
    string Uri,
    double Duration
);

public record HlsSegment(
    int Num,
    bool Init,
    bool Discontinuity,
    string Uri,
    double Duration,
    string? Title,
    Key? Key,
    ByteRange? ByteRange,
    DateTimeOffset? Date,
    Map? Map
) : Segment(Num, Init, Discontinuity, Uri, Duration);

public class M3U8
{
    public string? Uri { get; set; }
    public bool IsEndList { get; set; }
    public bool IsMaster { get; set; }
    public bool? AllowCache { get; set; }
    public int? DiscontinuitySequence { get; set; }
    public bool? IframesOnly { get; set; }
    public int? MediaSequence { get; set; }
    public string? PlaylistType { get; set; }
    public double? TargetDuration { get; set; }
    public Start? Start { get; set; }
    public int? Version { get; set; }

    public List<Media> Media { get; } = new();
    public List<DateRange> DateRanges { get; } = new();
    public List<HlsPlaylist> Playlists { get; } = new();
    public List<HlsSegment> Segments { get; } = new();

    public M3U8(string? uri = null)
    {
        Uri = uri;
    }
}
