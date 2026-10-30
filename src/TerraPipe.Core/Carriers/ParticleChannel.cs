using System.Buffers.Binary;

namespace TerraPipe.Carriers;

public sealed class ParticleChannel : ITerraChannel, IReceivingCarrier
{
    public const ushort ModuleParticles = 9;

    public const byte Magic = 0xB7;

    public const int Payload = 21;

    private readonly Action<byte[]> _sendRaw;

    public ParticleChannel(Action<byte[]> sendRaw) => _sendRaw = sendRaw;

    public int FrameSize => Payload;
    public string Name => "particles";

    public event Action<byte[]>? OnFrame;

    public void SendFrame(ReadOnlySpan<byte> frame)
    {
        if (frame.Length > Payload)
            throw new ArgumentException($"frame too large ({frame.Length} > {Payload})", nameof(frame));

        const int total = 3 + 2 + 1 + Payload;
        var pkt = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(pkt, (ushort)total);
        pkt[2] = 82;
        BinaryPrimitives.WriteUInt16LittleEndian(pkt.AsSpan(3), ModuleParticles);
        pkt[5] = Magic;
        frame.CopyTo(pkt.AsSpan(6));
        _sendRaw(pkt);
    }

    public bool Feed(byte packetType, ReadOnlySpan<byte> payload82)
    {
        if (packetType != 82) return false;
        if (payload82.Length < 3 + Payload) return false;
        if (BinaryPrimitives.ReadUInt16LittleEndian(payload82) != ModuleParticles) return false;
        if (payload82[2] != Magic) return false;
        var frame = payload82.Slice(3, Payload).ToArray();
        OnFrame?.Invoke(frame);
        return true;
    }
}
