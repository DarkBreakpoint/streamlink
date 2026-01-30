using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Streamlink.Hls.Library;

public class StreamBuffer
{
    private readonly Pipe _pipe;

    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;

    public StreamBuffer(PipeOptions? options = null)
    {
        _pipe = new Pipe(options ?? PipeOptions.Default);
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
    {
        await _pipe.Writer.WriteAsync(source, cancellationToken);
    }

    public void CompleteWriter(Exception? exception = null)
    {
        _pipe.Writer.Complete(exception);
    }

    public void CompleteReader(Exception? exception = null)
    {
        _pipe.Reader.Complete(exception);
    }
}
