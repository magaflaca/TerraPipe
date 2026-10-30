namespace TerraPipe.Protocol;

public enum FrameType : byte
{
    Open = 0x01,
    Data = 0x02,
    Ack = 0x03,
    Fin = 0x04,
    Done = 0x05,
    Abort = 0x06,
}

public static class PipeFrame
{
    public const int HeaderSize = 3;
    public const int MinFrameSize = 21;

    public const int TypeOffset = 0;
    public const int SenderOffset = 1;
    public const int StreamOffset = 2;
    public const int BodyOffset = 3;

    public const int DataBytes = MinFrameSize - HeaderSize - 2;
}
