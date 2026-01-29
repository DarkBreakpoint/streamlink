using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

public class M3U8Parser
{
    private readonly string? _baseUri;
    private readonly M3U8 _m3u8;

    // State
    private bool _expectPlaylist;
    private Dictionary<string, string>? _streamInf;
    private bool _expectSegment;
    private ExtInf? _extInf;
    private ByteRange? _byteRange;
    private bool _discontinuity;
    private Map? _map;
    private Key? _key;
    private DateTimeOffset? _date;

    public M3U8Parser(string? baseUri = null)
    {
        _baseUri = baseUri;
        _m3u8 = new M3U8(baseUri);
    }

    public M3U8 Parse(string data)
    {
        using var reader = new StringReader(data);
        return Parse(reader);
    }

    public M3U8 Parse(TextReader reader)
    {
        // Check header
        string? line = reader.ReadLine();
        if (line == null) return _m3u8;

        if (!line.StartsWith("#EXTM3U"))
        {
            // Log warning?
            throw new InvalidDataException("Missing #EXTM3U header");
        }

        while ((line = reader.ReadLine()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            ParseLine(line.AsSpan().Trim());
        }

        FinalizeParse();
        return _m3u8;
    }

    private void FinalizeParse()
    {
        _m3u8.IsMaster = _m3u8.Playlists.Count > 0;

        // Associate Media entries with each Playlist
        foreach (var playlist in _m3u8.Playlists)
        {
            var streamInfo = playlist.StreamInfo;
            // Handle StreamInfo
            // Python: group_id = getattr(playlist.stream_info, media_type, None)
            // We need to check Audio, Video, Subtitles group IDs.

            if (streamInfo is StreamInfo si)
            {
                if (si.Audio != null) AddMediaToPlaylist(playlist, si.Audio, "AUDIO");
                if (si.Video != null) AddMediaToPlaylist(playlist, si.Video, "VIDEO");
                if (si.Subtitles != null) AddMediaToPlaylist(playlist, si.Subtitles, "SUBTITLES");
            }
            else if (streamInfo is IFrameStreamInfo ifsi)
            {
                 if (ifsi.Video != null) AddMediaToPlaylist(playlist, ifsi.Video, "VIDEO");
            }
        }

        // Update segment numbers
        int mediaSequence = _m3u8.MediaSequence ?? 0;
        for (int i = 0; i < _m3u8.Segments.Count; i++)
        {
             // We can't easily modify the record since it is immutable...
             // But wait, records have `with`. But I stored them in a list.
             // I'll need to replace them or make Num mutable or set it correctly during creation.
             // M3U8 parsing usually happens sequentially so I should know the number when creating if I tracked it.
             // Python code does it at the end.
             // "Update segment numbers"
             // I will replace the segment in the list with the updated Num.
             var s = _m3u8.Segments[i];
             _m3u8.Segments[i] = s with { Num = mediaSequence + i };
        }
    }

    private void AddMediaToPlaylist(HlsPlaylist playlist, string groupId, string type)
    {
         var medias = _m3u8.Media.Where(m => m.GroupId == groupId).ToList(); // Type check? Python filters by group_id only.
         // Python: for media in filter(lambda m: m.group_id == group_id, self.m3u8.media): playlist.media.append(media)
         // Note: Python didn't filter by TYPE here inside the loop explicitly in the snippet I saw,
         // but `getattr(playlist.stream_info, media_type, None)` implies we look for the group ID corresponding to that type.
         // And `m.group_id` matching is enough.

         playlist.Media.AddRange(medias);
    }

    private void ParseLine(ReadOnlySpan<char> line)
    {
        if (line.StartsWith("#"))
        {
            SplitTag(line, out var tag, out var value);
            if (tag.IsEmpty) return;

            // Dispatch based on tag
            // Using switch on string (Span -> string) or hash
            // For high perf, maybe manual check or interned strings?
            // .NET optimizes switch on strings.
            string tagStr = tag.ToString(); // alloc
            string valStr = value.ToString(); // alloc - TODO: Optimize to avoid alloc if possible, but parsing attributes usually needs strings.

            switch (tagStr)
            {
                // Basic
                case "EXT-X-VERSION": _m3u8.Version = int.Parse(valStr); break;

                // Media Segment
                case "EXTINF": ParseExtInf(valStr); break;
                case "EXT-X-BYTERANGE":
                    _expectSegment = true;
                    _byteRange = ParseByteRange(valStr);
                    break;
                case "EXT-X-DISCONTINUITY":
                    _discontinuity = true;
                    _map = null;
                    break;
                case "EXT-X-KEY": ParseKey(valStr); break;
                case "EXT-X-MAP": ParseMap(valStr); break;
                case "EXT-X-PROGRAM-DATE-TIME": _date = DateTimeOffset.Parse(valStr); break; // ISO8601
                case "EXT-X-DATERANGE": ParseDateRange(valStr); break;

                // Media Playlist
                case "EXT-X-TARGETDURATION": _m3u8.TargetDuration = double.Parse(valStr, CultureInfo.InvariantCulture); break;
                case "EXT-X-MEDIA-SEQUENCE": _m3u8.MediaSequence = int.Parse(valStr); break;
                case "EXT-X-DISCONTINUITY-SEQUENCE": _m3u8.DiscontinuitySequence = int.Parse(valStr); break;
                case "EXT-X-ENDLIST": _m3u8.IsEndList = true; break;
                case "EXT-X-PLAYLIST-TYPE": _m3u8.PlaylistType = valStr; break;
                case "EXT-X-I-FRAMES-ONLY": _m3u8.IframesOnly = true; break;
                case "EXT-X-ALLOW-CACHE": _m3u8.AllowCache = valStr == "YES"; break;

                // Master Playlist
                case "EXT-X-MEDIA": ParseMedia(valStr); break;
                case "EXT-X-STREAM-INF":
                    _expectPlaylist = true;
                    _streamInf = ParseAttributes(valStr);
                    break;
                case "EXT-X-I-FRAME-STREAM-INF": ParseIFrameStreamInf(valStr); break;
                case "EXT-X-START": ParseStart(valStr); break;
            }
        }
        else if (_expectSegment)
        {
            _expectSegment = false;
            var segment = GetSegment(ResolveUri(line.ToString()));
            _m3u8.Segments.Add(segment);
        }
        else if (_expectPlaylist)
        {
            _expectPlaylist = false;
            var playlist = GetPlaylist(ResolveUri(line.ToString()));
            _m3u8.Playlists.Add(playlist);
        }
    }

    private void SplitTag(ReadOnlySpan<char> line, out ReadOnlySpan<char> tag, out ReadOnlySpan<char> value)
    {
        // #TAG:VALUE
        // remove #
        var content = line.Slice(1);
        int idx = content.IndexOf(':');
        if (idx == -1)
        {
            tag = content;
            value = ReadOnlySpan<char>.Empty;
        }
        else
        {
            tag = content.Slice(0, idx);
            value = content.Slice(idx + 1);
        }
    }

    private Dictionary<string, string> ParseAttributes(string value)
    {
        // Regex is heavy. Manual parsing.
        // Key=Value,Key="Value",...
        var result = new Dictionary<string, string>();
        var span = value.AsSpan();
        int i = 0;
        while (i < span.Length)
        {
            // Skip whitespace
            while (i < span.Length && char.IsWhiteSpace(span[i])) i++;
            if (i >= span.Length) break;

            // Parse Key
            int keyStart = i;
            while (i < span.Length && span[i] != '=') i++;
            if (i >= span.Length) break; // Invalid?

            var key = span.Slice(keyStart, i - keyStart).Trim().ToString();
            i++; // Skip =

            // Parse Value
            string val;
            if (i < span.Length && span[i] == '"')
            {
                // Quoted string
                i++;
                int valStart = i;
                while (i < span.Length && span[i] != '"') i++; // Handle escaped quotes? HLS spec says no escaped quotes usually?
                val = span.Slice(valStart, i - valStart).ToString();
                i++; // Skip closing quote
            }
            else
            {
                int valStart = i;
                while (i < span.Length && span[i] != ',') i++;
                val = span.Slice(valStart, i - valStart).Trim().ToString();
            }

            result[key] = val;

            // Skip comma
            while (i < span.Length && (char.IsWhiteSpace(span[i]) || span[i] == ',')) i++;
        }
        return result;
    }

    private void ParseExtInf(string value)
    {
        _expectSegment = true;
        // duration,title
        int idx = value.IndexOf(',');
        if (idx == -1)
        {
             if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
             {
                 _extInf = new ExtInf(d, null);
             }
             else
             {
                 _extInf = new ExtInf(0, null);
             }
        }
        else
        {
             double.TryParse(value.Substring(0, idx), NumberStyles.Any, CultureInfo.InvariantCulture, out double d);
             string title = value.Substring(idx + 1);
             _extInf = new ExtInf(d, title);
        }
    }

    private ByteRange ParseByteRange(string value)
    {
        // length[@offset]
        int idx = value.IndexOf('@');
        if (idx == -1)
        {
            return new ByteRange(long.Parse(value), null);
        }
        else
        {
            return new ByteRange(long.Parse(value.Substring(0, idx)), long.Parse(value.Substring(idx + 1)));
        }
    }

    private void ParseKey(string value)
    {
        var attrs = ParseAttributes(value);
        if (!attrs.TryGetValue("METHOD", out var method)) return;

        string? uri = attrs.GetValueOrDefault("URI");
        string? ivStr = attrs.GetValueOrDefault("IV");
        byte[]? iv = null;
        if (ivStr != null)
        {
            if (ivStr.StartsWith("0x") || ivStr.StartsWith("0X"))
            {
                iv = Convert.FromHexString(ivStr.AsSpan(2));
            }
        }

        _key = new Key(method, ResolveUri(uri), iv, attrs.GetValueOrDefault("KEYFORMAT"), attrs.GetValueOrDefault("KEYFORMATVERSIONS"));
    }

    private void ParseMap(string value)
    {
        var attrs = ParseAttributes(value);
        string? uri = attrs.GetValueOrDefault("URI");
        if (uri == null) return;

        ByteRange? br = null;
        if (attrs.TryGetValue("BYTERANGE", out var brVal))
        {
            br = ParseByteRange(brVal);
        }

        _map = new Map(ResolveUri(uri), _key, br);
    }

    private void ParseDateRange(string value)
    {
        var attrs = ParseAttributes(value);
        // ID, CLASS, START-DATE, END-DATE, DURATION, PLANNED-DURATION, END-ON-NEXT

        var dr = new DateRange(
            attrs.GetValueOrDefault("ID"),
            attrs.GetValueOrDefault("CLASS"),
            ParseIso(attrs.GetValueOrDefault("START-DATE")),
            ParseIso(attrs.GetValueOrDefault("END-DATE")),
            ParseTimeSpan(attrs.GetValueOrDefault("DURATION")),
            ParseTimeSpan(attrs.GetValueOrDefault("PLANNED-DURATION")),
            attrs.GetValueOrDefault("END-ON-NEXT") == "YES",
            attrs
        );
        _m3u8.DateRanges.Add(dr);
    }

    private DateTimeOffset? ParseIso(string? val) => val == null ? null : DateTimeOffset.Parse(val);
    private TimeSpan? ParseTimeSpan(string? val) => val == null ? null : TimeSpan.FromSeconds(double.Parse(val, CultureInfo.InvariantCulture));

    private void ParseMedia(string value)
    {
        var attrs = ParseAttributes(value);
        var type = attrs.GetValueOrDefault("TYPE");
        var groupId = attrs.GetValueOrDefault("GROUP-ID");
        var name = attrs.GetValueOrDefault("NAME");

        if (type == null || groupId == null || name == null) return;

        var media = new Media(
            ResolveUri(attrs.GetValueOrDefault("URI")),
            type,
            groupId,
            attrs.GetValueOrDefault("LANGUAGE")?.ToLowerInvariant(),
            name,
            attrs.GetValueOrDefault("DEFAULT") == "YES",
            attrs.GetValueOrDefault("AUTOSELECT") == "YES",
            attrs.GetValueOrDefault("FORCED") == "YES",
            attrs.GetValueOrDefault("CHARACTERISTICS")
        );
        _m3u8.Media.Add(media);
    }

    private void ParseStart(string value)
    {
        var attrs = ParseAttributes(value);
        if (double.TryParse(attrs.GetValueOrDefault("TIME-OFFSET"), NumberStyles.Any, CultureInfo.InvariantCulture, out double offset))
        {
            _m3u8.Start = new Start(offset, attrs.GetValueOrDefault("PRECISE") == "YES");
        }
    }

    private void ParseIFrameStreamInf(string value)
    {
        var attrs = ParseAttributes(value);
        var uri = attrs.GetValueOrDefault("URI");

        var streamInfAttrs = _streamInf ?? attrs;
        _streamInf = null;

        if (uri == null) return;

        var si = CreateIFrameStreamInfo(streamInfAttrs);
        var playlist = new HlsPlaylist(ResolveUri(uri), si, new List<Media>(), true);
        _m3u8.Playlists.Add(playlist);
    }

    private IFrameStreamInfo CreateIFrameStreamInfo(Dictionary<string, string> attrs)
    {
         int bandwidth = int.Parse(attrs.GetValueOrDefault("BANDWIDTH", "0"));
         bandwidth = RoundBandwidth(bandwidth);

         return new IFrameStreamInfo(
             bandwidth,
             attrs.GetValueOrDefault("PROGRAM-ID"),
             attrs.GetValueOrDefault("CODECS", "").Split(',').Where(s => !string.IsNullOrEmpty(s)).ToList(),
             ParseResolution(attrs.GetValueOrDefault("RESOLUTION")),
             attrs.GetValueOrDefault("VIDEO")
         );
    }

    private HlsPlaylist GetPlaylist(string uri)
    {
        var attrs = _streamInf ?? new Dictionary<string, string>();
        _streamInf = null;

        var si = CreateStreamInfo(attrs);
        return new HlsPlaylist(uri, si, new List<Media>(), false);
    }

    private StreamInfo CreateStreamInfo(Dictionary<string, string> attrs)
    {
         int bandwidth = int.Parse(attrs.GetValueOrDefault("BANDWIDTH", "0"));
         bandwidth = RoundBandwidth(bandwidth);

         return new StreamInfo(
             bandwidth,
             attrs.GetValueOrDefault("PROGRAM-ID"),
             attrs.GetValueOrDefault("CODECS", "").Split(',').Where(s => !string.IsNullOrEmpty(s)).ToList(),
             ParseResolution(attrs.GetValueOrDefault("RESOLUTION")),
             attrs.GetValueOrDefault("AUDIO"),
             attrs.GetValueOrDefault("VIDEO"),
             attrs.GetValueOrDefault("SUBTITLES")
         );
    }

    private int RoundBandwidth(int bandwidth)
    {
        if (bandwidth == 0) return 0;
        // bandwidth = round(bandwidth, 1 - int(math.log10(bandwidth)))
        // In C#:
        int digits = 1 - (int)Math.Log10(bandwidth);
        // round(number, ndigits)
        // If ndigits is negative, it rounds to tens, hundreds, etc.
        // C# Math.Round doesn't support negative decimals for power of 10 rounding directly like Python.
        // We need to scale.
        double scale = Math.Pow(10, -digits); // e.g. digits = -3 (round to 1000s) -> scale = 1000
        return (int)(Math.Round(bandwidth / scale) * scale);
    }

    private Resolution? ParseResolution(string? res)
    {
        if (res == null) return null;
        var parts = res.Split('x');
        if (parts.Length == 2 && int.TryParse(parts[0], out int w) && int.TryParse(parts[1], out int h))
        {
            return new Resolution(w, h);
        }
        return new Resolution(0, 0);
    }

    private HlsSegment GetSegment(string uri)
    {
        var extInf = _extInf ?? new ExtInf(0, null);
        _extInf = null;

        var discontinuity = _discontinuity;
        _discontinuity = false;

        var byteRange = _byteRange;
        _byteRange = null;

        var date = _date;
        _date = null;

        return new HlsSegment(
            -1, // Num filled later
            false, // Init?
            discontinuity,
            uri,
            extInf.Duration,
            extInf.Title,
            _key,
            byteRange,
            date,
            _map
        );
    }

    private string ResolveUri(string? uri)
    {
        if (string.IsNullOrEmpty(uri)) return "";
        if (IsAbsoluteUri(uri)) return uri;
        if (string.IsNullOrEmpty(_baseUri)) return uri;

        // Combine
        // System.Uri handles this well
        if (Uri.TryCreate(new Uri(_baseUri), uri, out var result))
        {
            return result.ToString();
        }
        return uri;
    }

    private bool IsAbsoluteUri(string uri)
    {
         return Uri.TryCreate(uri, UriKind.Absolute, out _);
    }
}
