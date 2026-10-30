using System.Buffers.Binary;

namespace TerraPipe.Carriers;

public sealed class PingChannel : ITerraChannel, IReceivingCarrier
{
    public const ushort ModulePing = 2;
    private const int PerPing = 7;
    private const int Frags = 4;
    private const int FrameBytes = 3 * PerPing;
    private static readonly TimeSpan PartialTtl = TimeSpan.FromMilliseconds(1200);

    private readonly Action<byte[]> _sendRaw;
    private int _sfid;
    private readonly Dictionary<int, Partial> _inbox = new();

    public PingChannel(Action<byte[]> sendRaw) => _sendRaw = sendRaw;

    public int FrameSize => FrameBytes;
    public string Name => "ping";
    public event Action<byte[]>? OnFrame;

    private sealed class Partial { public readonly byte[]?[] Parts = new byte[Frags][]; public DateTime Touch = DateTime.UtcNow; }

    private static byte Crc8(ReadOnlySpan<byte> d)
    {
        byte crc = 0;
        foreach (byte b in d) { crc ^= b; for (int i = 0; i < 8; i++) crc = (byte)((crc & 0x80) != 0 ? crc << 1 ^ 0x07 : crc << 1); }
        return crc;
    }

    public void SendFrame(ReadOnlySpan<byte> frame)
    {
        if (frame.Length > FrameBytes) throw new ArgumentException($"frame too large ({frame.Length})", nameof(frame));
        Span<byte> full = stackalloc byte[FrameBytes];
        frame.CopyTo(full);
        int sfid = _sfid; _sfid = (_sfid + 1) & 0x3F;
        byte crc = Crc8(full);

        Span<byte> eight = stackalloc byte[8];
        for (int i = 0; i < Frags; i++)
        {
            eight.Clear();
            eight[0] = (byte)((sfid & 0x3F) << 2 | (i & 0x03));
            if (i < 3) full.Slice(i * PerPing, PerPing).CopyTo(eight[1..]);
            else eight[1] = crc;
            _sendRaw(BuildPingPacket(eight));
        }
    }

    private static byte[] BuildPingPacket(ReadOnlySpan<byte> eight)
    {
        const int total = 3 + 2 + 8;
        var pkt = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(pkt, total);
        pkt[2] = 82;
        BinaryPrimitives.WriteUInt16LittleEndian(pkt.AsSpan(3), ModulePing);
        eight.CopyTo(pkt.AsSpan(5));
        return pkt;
    }

    public bool Feed(byte packetType, ReadOnlySpan<byte> payload82)
    {
        if (packetType != 82) return false;
        if (payload82.Length < 2 + 8) return false;
        if (BinaryPrimitives.ReadUInt16LittleEndian(payload82) != ModulePing) return false;
        var eight = payload82.Slice(2, 8);
        byte h = eight[0];
        int sfid = (h >> 2) & 0x3F;
        int frag = h & 0x03;

        var now = DateTime.UtcNow;
        if (_inbox.Count > 0)
        {
            var dead = _inbox.Where(kv => now - kv.Value.Touch > PartialTtl).Select(kv => kv.Key).ToList();
            foreach (var k in dead) _inbox.Remove(k);
        }

        if (!_inbox.TryGetValue(sfid, out var part)) { part = new Partial(); _inbox[sfid] = part; }
        part.Parts[frag] = eight.Slice(1, PerPing).ToArray();
        part.Touch = now;

        if (part.Parts[0] is null || part.Parts[1] is null || part.Parts[2] is null || part.Parts[3] is null) return true;

        var frame = new byte[FrameBytes];
        for (int i = 0; i < 3; i++) part.Parts[i]!.CopyTo(frame, i * PerPing);
        _inbox.Remove(sfid);
        if (Crc8(frame) == part.Parts[3]![0]) OnFrame?.Invoke(frame);
        return true;
    }
}
