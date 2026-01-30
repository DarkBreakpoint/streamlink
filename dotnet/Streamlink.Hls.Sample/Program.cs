using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Streamlink.Hls.Library;

namespace Streamlink.Hls.Sample;

class Program
{
    static async Task Main(string[] args)
    {
        // Setup DI
        var services = new ServiceCollection();
        services.AddLogging(configure => configure.AddConsole());
        services.AddSingleton<IHlsSession, HlsSession>();
        services.AddHttpClient();

        // Sample URLs (using public test streams or generic examples)
        // These might fail if not real, but demonstrate the code structure.
        var feedUrls = new[]
        {
            "http://sample.vodobox.net/skate_phantom_flex_4k/skate_phantom_flex_4k.m3u8", // Example 1
            "http://playertest.longtailvideo.com/adaptive/wowzaid3/playlist.m3u8", // Example 2
            "http://content.jwplatform.com/manifests/vM7nH0Kl.m3u8", // Example 3
            "http://assets.afcdn.com/video49/20210722/v_645516/v_645516_720p.m3u8" // Example 4
        };

        using var provider = services.BuildServiceProvider();
        var session = provider.GetRequiredService<IHlsSession>();

        Console.WriteLine("Starting 4 concurrent HLS downloads...");

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, e) => { e.Cancel = true; cts.Cancel(); };

        var tasks = new List<Task>();

        for (int i = 0; i < feedUrls.Length; i++)
        {
            int index = i;
            string url = feedUrls[i];
            tasks.Add(Task.Run(() => DownloadFeedAsync(index, url, session, cts.Token)));
        }

        await Task.WhenAll(tasks);
        Console.WriteLine("All downloads completed.");
    }

    static async Task DownloadFeedAsync(int id, string url, IHlsSession session, CancellationToken ct)
    {
        try
        {
            Console.WriteLine($"[Feed {id}] Connecting to {url}...");
            var stream = new HlsStream(session, url);
            using var inputStream = await stream.OpenAsync(ct);

            // Consume stream and calculate stats
            var buffer = new byte[8192];
            long totalBytes = 0;
            var sw = Stopwatch.StartNew();
            var lastLog = sw.ElapsedMilliseconds;

            while (true)
            {
                int read = await inputStream.ReadAsync(buffer, ct);
                if (read == 0) break;
                totalBytes += read;

                if (sw.ElapsedMilliseconds - lastLog > 2000)
                {
                    double speed = totalBytes / 1024.0 / 1024.0 / sw.Elapsed.TotalSeconds;
                    Console.WriteLine($"[Feed {id}] Read {totalBytes / 1024.0:F2} KB. Speed: {speed:F2} MB/s");
                    lastLog = sw.ElapsedMilliseconds;
                }
            }

            Console.WriteLine($"[Feed {id}] Completed. Total: {totalBytes / 1024.0 / 1024.0:F2} MB.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Feed {id}] Error: {ex.Message}");
        }
    }
}
