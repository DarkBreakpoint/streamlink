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
            if (arg == "-o" || arg == "--output")
            {
                if (i + 1 < args.Length) outputPath = args[++i];
            }
            else if (arg == "--live-edge")
            {
                if (i + 1 < args.Length && double.TryParse(args[++i], out var v)) options.LiveEdge = v;
            }
            else if (arg == "--start-offset")
            {
                if (i + 1 < args.Length && double.TryParse(args[++i], out var v)) options.StartOffset = v;
            }
            else if (arg == "--duration")
            {
                if (i + 1 < args.Length && double.TryParse(args[++i], out var v)) options.Duration = v;
            }
            else if (arg == "--retries")
            {
                if (i + 1 < args.Length && int.TryParse(args[++i], out var v)) options.Retries = v;
            }
            else if (arg == "--timeout")
            {
                if (i + 1 < args.Length && int.TryParse(args[++i], out var v)) options.StreamTimeout = v;
            }
            else if (arg == "--live-restart")
            {
                options.LiveRestart = true;
            }
            else if (!arg.StartsWith("-"))
            {
                url = arg;
            }
        }

        if (string.IsNullOrEmpty(url))
        {
            Console.WriteLine("Usage: Streamlink.Hls.Cli <url> [options]");
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
