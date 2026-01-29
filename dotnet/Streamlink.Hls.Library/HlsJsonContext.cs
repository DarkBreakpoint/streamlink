using System.Text.Json.Serialization;
using Streamlink.Hls.Library.Models;

namespace Streamlink.Hls.Library;

[JsonSerializable(typeof(HlsOptions))]
[JsonSerializable(typeof(M3U8))]
[JsonSerializable(typeof(HlsSegment))]
[JsonSerializable(typeof(HlsPlaylist))]
public partial class HlsJsonContext : JsonSerializerContext
{
}
