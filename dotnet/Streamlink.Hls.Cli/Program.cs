using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Streamlink.Hls.Library;

namespace Streamlink.Hls.Cli;

class Program
{
    static async Task<int> Main(string[] args)
    {
        // 1. Parse Arguments
        var options = new HlsOptions();
        string? url = null;
        string? outputPath = null;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            // Value helper
            bool HasVal(out string val)
            {
                if (i + 1 < args.Length)
                {
                    val = args[++i];
                    return true;
                }
                val = "";
                return false;
            }

            if (arg == "-o" || arg == "--output")
            {
                if (HasVal(out var v)) outputPath = v;
            }
            else if (arg == "--hls-live-edge")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.LiveEdge = d;
            }
            else if (arg == "--hls-start-offset")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StartOffset = d;
            }
            else if (arg == "--hls-duration")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.Duration = d;
            }
            else if (arg == "--stream-segment-attempts")
            {
                if (HasVal(out var v) && int.TryParse(v, out var d)) options.StreamSegmentAttempts = d;
            }
            else if (arg == "--stream-segment-threads")
            {
                if (HasVal(out var v) && int.TryParse(v, out var d)) options.StreamSegmentThreads = d;
            }
            else if (arg == "--stream-segment-timeout")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StreamSegmentTimeout = d;
            }
            else if (arg == "--stream-timeout")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StreamTimeout = d;
            }
            else if (arg == "--stream-stall-timeout")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StreamStallTimeout = d;
            }
            else if (arg == "--stream-segmented-duration")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StreamSegmentedDuration = d;
            }
            else if (arg == "--stream-segmented-queue-deadline")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StreamSegmentedQueueDeadline = d;
            }
            else if (arg == "--hls-segment-queue-threshold")
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.HlsSegmentQueueThreshold = d;
            }
            else if (arg == "--hls-playlist-reload-attempts")
            {
                if (HasVal(out var v) && int.TryParse(v, out var d)) options.PlaylistReloadAttempts = d;
            }
            else if (arg == "--hls-segment-key-uri")
            {
                if (HasVal(out var v)) options.SegmentKeyUriOverride = v;
            }
            else if (arg == "--hls-segment-ignore-names")
            {
                if (HasVal(out var v)) options.SegmentIgnoreNames.Add(v);
            }
            else if (arg == "--hls-audio-select")
            {
                if (HasVal(out var v)) options.HlsAudioSelect.Add(v);
            }
            else if (arg == "--hls-live-restart")
            {
                options.LiveRestart = true;
            }
            else if (arg == "--kick-low-latency")
            {
                options.KickLowLatency = true;
            }
            else if (arg == "--hls-segment-stream-data")
            {
                options.SegmentStreamData = true;
            }
            else if (arg == "--live-edge") // Alias
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.LiveEdge = d;
            }
            else if (arg == "--start-offset") // Alias
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.StartOffset = d;
            }
            else if (arg == "--duration") // Alias
            {
                if (HasVal(out var v) && double.TryParse(v, out var d)) options.Duration = d;
            }
            else if (!arg.StartsWith("-"))
            {
                url = arg;
            }
        }

        if (string.IsNullOrEmpty(url))
        {
            Console.WriteLine("Usage: Streamlink.Hls.Cli <url> [options]");
            Console.WriteLine("Options:");
            Console.WriteLine("  --output <path>                  Write stream to file");
            Console.WriteLine("  --hls-live-edge <n>              Segments from live edge to start (default 3)");
            Console.WriteLine("  --hls-start-offset <n>           Start offset in seconds");
            Console.WriteLine("  --hls-duration <n>               Stop after n seconds");
            Console.WriteLine("  --stream-segment-attempts <n>    Retries per segment (default 3)");
            Console.WriteLine("  --stream-segment-threads <n>     Concurrent threads (default 1)");
            Console.WriteLine("  --stream-segment-timeout <n>     Timeout per segment (default 10.0)");
            Console.WriteLine("  --stream-timeout <n>             Connection timeout (default 60.0)");
            Console.WriteLine("  --stream-stall-timeout <n>       Data read stall timeout (default 5.0)");
            Console.WriteLine("  --hls-playlist-reload-attempts <n>");
            Console.WriteLine("  --hls-live-restart               Start from beginning of live stream");
            Console.WriteLine("  --kick-low-latency               Reduce live edge for low latency");
            return 1;
        }

        // 2. Setup DI
        var services = new ServiceCollection();
        services.AddLogging(configure => configure.AddConsole());
        services.AddSingleton(options);
        services.AddHttpClient();
        services.AddSingleton<IHlsSession, HlsSession>();
        services.AddTransient<HlsStream>(sp =>
        {
             var session = sp.GetRequiredService<IHlsSession>();
             return new HlsStream(session, url);
        });

        using var serviceProvider = services.BuildServiceProvider();
        var stream = serviceProvider.GetRequiredService<HlsStream>();

        // 3. Run
        try
        {
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            using var inputStream = await stream.OpenAsync(cts.Token);

            Stream outputStream;
            if (string.IsNullOrEmpty(outputPath) || outputPath == "-")
            {
                outputStream = Console.OpenStandardOutput();
            }
            else
            {
                outputStream = File.OpenWrite(outputPath);
            }

            using (outputStream)
            {
                await inputStream.CopyToAsync(outputStream, cts.Token);
            }

            return 0;
        }
        catch (OperationCanceledException)
        {
            return 0; // Cancelled
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }
}
