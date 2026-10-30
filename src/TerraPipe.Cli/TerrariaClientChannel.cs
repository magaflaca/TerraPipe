using TerraPipe.Carriers;
using TerrariaHeadless;

namespace TerraPipe.Cli;

public enum CarrierKind { Particle, Ping }

public sealed class TerrariaClientChannel : IDisposable
{
    private readonly TerrariaClient _client;
    private readonly IReceivingCarrier _rx;
    public ITerraChannel Channel { get; }

    public TerrariaClientChannel(TerrariaClient client, CarrierKind kind = CarrierKind.Particle)
    {
        _client = client;
        ITerraChannel ch = kind switch
        {
            CarrierKind.Ping => new PingChannel(client.Send),
            _ => new ParticleChannel(client.Send),
        };
        Channel = ch;
        _rx = (IReceivingCarrier)ch;
        _client.OnRawPacket += OnRaw;
    }

    private void OnRaw(byte type, ReadOnlyMemory<byte> payload) => _rx.Feed(type, payload.Span);

    public void Dispose() => _client.OnRawPacket -= OnRaw;
}
