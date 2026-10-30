namespace TerraPipe.Carriers;

public interface ITerraChannel
{
    int FrameSize { get; }

    string Name { get; }

    void SendFrame(ReadOnlySpan<byte> frame);

    event Action<byte[]>? OnFrame;
}

public interface IReceivingCarrier
{
    bool Feed(byte packetType, ReadOnlySpan<byte> payload);
}
