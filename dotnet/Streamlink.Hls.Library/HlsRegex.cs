using System.Text.RegularExpressions;

namespace Streamlink.Hls.Library;

public static partial class HlsRegex
{
    // Example: A regex for filtering names if users provide a pattern
    // [GeneratedRegex] requires partial method
    [GeneratedRegex(".*", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    public static partial Regex Any();

    // If user provides a regex pattern string, we can't fully source generate it dynamically at runtime.
    // However, if we had known patterns, we would generate them here.
    // For dynamic patterns from CLI options, we must fall back to the runtime engine (which is AOT compatible but slower startup)
    // or use a limited set of pre-compiled patterns.
    // The request was "If we introduce any Regex...".

    // I will add a helper to validate if we can use generated regex for common patterns.
}
