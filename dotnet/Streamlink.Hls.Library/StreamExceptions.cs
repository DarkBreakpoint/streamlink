using System;

namespace Streamlink.Hls.Library;

public class StreamError : Exception
{
    public StreamError(string message) : base(message) { }
    public StreamError(string message, Exception inner) : base(message, inner) { }
}
