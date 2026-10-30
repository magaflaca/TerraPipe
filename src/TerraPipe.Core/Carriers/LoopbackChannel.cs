namespace TerraPipe.Carriers;

public sealed class LoopbackHub
{
    private readonly List<LoopbackChannel> _endpoints = new();
    private readonly object _sync = new();
    public double LossRate;
    public int MaxLatencyMs;
    private readonly Random _rng = new(12345);

    public LoopbackChannel CreateEndpoint(int frameSize = 21)
    {
        var ep = new LoopbackChannel(this, frameSize);
        lock (_sync) _endpoints.Add(ep);
        return ep;
    }

    internal void Relay(LoopbackChannel from, byte[] frame)
    {
        List<LoopbackChannel> others;
        lock (_sync) others = _endpoints.Where(e => e != from).ToList();
        foreach (var e in others)
        {
            double loss; int delay;
            lock (_sync) { loss = _rng.NextDouble(); delay = MaxLatencyMs > 0 ? _rng.Next(MaxLatencyMs) : 0; }
            if (loss < LossRate) continue;
            if (delay > 0) Task.Delay(delay).ContinueWith(_ => e.Deliver(frame));
            else e.Deliver(frame);
        }
    }
}

public sealed class LoopbackChannel : ITerraChannel
{
    private readonly LoopbackHub _hub;
    public LoopbackChannel(LoopbackHub hub, int frameSize) { _hub = hub; FrameSize = frameSize; }

    public int FrameSize { get; }
    public string Name => "loopback";
    public event Action<byte[]>? OnFrame;

    public void SendFrame(ReadOnlySpan<byte> frame) => _hub.Relay(this, frame.ToArray());
    internal void Deliver(byte[] frame) => OnFrame?.Invoke(frame);
}
