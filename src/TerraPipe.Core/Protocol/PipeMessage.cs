using System.Buffers.Binary;
using System.Text;

namespace TerraPipe.Protocol;

public enum PipeKind : byte { Raw = 0, Text = 1, File = 2, Image = 3 }

public sealed record PipeMessage(PipeKind Kind, string Name, byte[] Data)
{
    private static readonly byte[] Magic = "TP"u8.ToArray();
    public const byte Version = 1;

    public byte[] Encode()
    {
        var name = Encoding.UTF8.GetBytes(Name);
        if (name.Length > 255) name = name[..255];
        var buf = new byte[Magic.Length + 1 + 1 + 1 + name.Length + 4 + Data.Length];
        int o = 0;
        Magic.CopyTo(buf, o); o += Magic.Length;
        buf[o++] = Version;
        buf[o++] = (byte)Kind;
        buf[o++] = (byte)name.Length;
        name.CopyTo(buf, o); o += name.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(o), (uint)Data.Length); o += 4;
        Data.CopyTo(buf, o);
        return buf;
    }

    public static bool TryDecode(byte[] blob, out PipeMessage msg)
    {
        msg = default!;
        var s = blob.AsSpan();
        if (s.Length < Magic.Length + 3 || !s[..Magic.Length].SequenceEqual(Magic)) return false;
        int o = Magic.Length;
        if (s[o++] != Version) return false;
        var kind = (PipeKind)s[o++];
        int nameLen = s[o++];
        if (s.Length < o + nameLen + 4) return false;
        string name = Encoding.UTF8.GetString(s.Slice(o, nameLen)); o += nameLen;
        long dataLen = BinaryPrimitives.ReadUInt32LittleEndian(s[o..]); o += 4;
        if (s.Length < o + dataLen) return false;
        msg = new PipeMessage(kind, name, s.Slice(o, (int)dataLen).ToArray());
        return true;
    }

    public static PipeMessage Text(string text) => new(PipeKind.Text, "", Encoding.UTF8.GetBytes(text));
    public static PipeMessage FromFile(string path)
    {
        var data = File.ReadAllBytes(path);
        var ext = Path.GetExtension(path).ToLowerInvariant();
        var kind = ext is ".png" or ".jpg" or ".jpeg" or ".gif" or ".bmp" or ".webp" ? PipeKind.Image : PipeKind.File;
        return new PipeMessage(kind, Path.GetFileName(path), data);
    }
}
