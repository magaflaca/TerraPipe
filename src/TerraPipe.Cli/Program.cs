using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using TerraPipe.Carriers;
using TerraPipe.Cli;
using TerraPipe.Protocol;
using TerrariaHeadless;
using TerrariaHeadless.Model;

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
if (args.Length == 0) { Usage(); return 1; }

try
{
    switch (args[0])
    {
        case "probe": return await Probe(Rest(args));
        case "selftest": return await SelfTest(Rest(args));
        case "send": return await Send(Rest(args));
        case "recv": return await Recv(Rest(args));
        case "livetest": return await LiveTest(Rest(args));
        case "scan": return await Scan(Rest(args));
        case "scandist": return await ScanDist(Rest(args));
        default: Usage(); return 1;
    }
}
catch (Exception ex) { Console.Error.WriteLine("error: " + ex.Message); return 10; }

static string[] Rest(string[] a) => a.Skip(1).ToArray();

static void Usage() => Console.Error.WriteLine(
    "terrapipe <command>\n" +
    "  probe    <host:port>                              verify the stealth carrier relays between two clients\n" +
    "  selftest [--loss 0.1] [--reorder 40] [--size N]   run the full protocol over an in-memory lossy channel\n" +
    "  send     <host:port> --name N (--file P | --text S) [--rate 60] [--tag T]\n" +
    "  recv     <host:port> [--out DIR] [--once]\n");

static Dictionary<string, string> Flags(string[] a, out List<string> pos)
{
    var d = new Dictionary<string, string>(); pos = new();
    for (int i = 0; i < a.Length; i++)
        if (a[i].StartsWith("--")) { var k = a[i][2..]; var v = i + 1 < a.Length && !a[i + 1].StartsWith("--") ? a[++i] : "true"; d[k] = v; }
        else pos.Add(a[i]);
    return d;
}

static (string host, int port) ParseHost(string s)
{ var i = s.LastIndexOf(':'); return i < 0 ? (s, 7777) : (s[..i], int.Parse(s[(i + 1)..])); }

static CarrierKind ParseCarrier(Dictionary<string, string> f) => f.GetValueOrDefault("carrier", "particle").ToLowerInvariant() switch
{
    "ping" => CarrierKind.Ping,
    _ => CarrierKind.Particle,
};

static TerrariaClient MakeClient(string host, int port, string name) => new(new ClientOptions
{ Host = host, Port = port, PlayerName = name, Uuid = Guid.NewGuid().ToString(), StarterItems = false, RunTickLoop = true });

static async Task<int> Probe(string[] a)
{
    var (host, port) = ParseHost(a.Length > 0 ? a[0] : "127.0.0.1:7787");
    Console.WriteLine($"[probe] server {host}:{port}");
    await using var rx = MakeClient(host, port, "TPrx");
    await using var tx = MakeClient(host, port, "TPtx");
    var got = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    using var rxCh = new TerrariaClientChannel(rx);
    rxCh.Channel.OnFrame += f => got.TrySetResult(f);
    await rx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(30));
    using var txCh = new TerrariaClientChannel(tx);
    await tx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(30));
    Console.WriteLine($"[probe] rx=index {rx.MyIndex}, tx=index {tx.MyIndex}; sending stealth frame...");
    var payload = new byte[ParticleChannel.Payload];
    for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(i * 11 + 1);
    payload[7] = 0x00; payload[8] = 0xFF;
    for (int k = 0; k < 5 && !got.Task.IsCompleted; k++) { txCh.Channel.SendFrame(payload); await Task.WhenAny(got.Task, Task.Delay(1000)); }
    if (!got.Task.IsCompleted) { Console.Error.WriteLine("[probe] FAIL: not relayed"); return 2; }
    bool ok = got.Task.Result.AsSpan().SequenceEqual(payload);
    Console.WriteLine($"[probe] {(ok ? "PASS" : "FAIL")}: {Convert.ToHexString(got.Task.Result)}");
    return ok ? 0 : 3;
}

static async Task<int> SelfTest(string[] a)
{
    var f = Flags(a, out _);
    double loss = f.TryGetValue("loss", out var l) ? double.Parse(l) : 0.15;
    int reorder = f.TryGetValue("reorder", out var r) ? int.Parse(r) : 40;
    int size = f.TryGetValue("size", out var s) ? int.Parse(s) : 20000;

    var carrier = ParseCarrier(f);
    int rate = f.TryGetValue("rate", out var rt) ? int.Parse(rt) : 2000;
    var hub = new RawLoopback { LossRate = loss, MaxLatencyMs = reorder };
    var sCh = hub.CreateCarrier(carrier, spawner: 1);
    var rCh = hub.CreateCarrier(carrier, spawner: 2);
    using var sSession = new PipeSession(sCh, new PipeOptions { SendRatePerSecond = rate });
    using var rSession = new PipeSession(rCh, new PipeOptions { SendRatePerSecond = rate });

    var data = new byte[size];
    new Random(7).NextBytes(data);
    var msg = new PipeMessage(PipeKind.Image, "test.png", data);

    Console.WriteLine($"[selftest] carrier={carrier} loss={loss} reorder={reorder}ms size={size}B (frame={sCh.FrameSize}B)");
    var recvTask = rSession.ReceiveAsync();
    var sw = Stopwatch.StartNew();
    await sSession.SendAsync(msg.Encode());
    var blob = await recvTask;
    sw.Stop();

    if (!PipeMessage.TryDecode(blob, out var got)) { Console.Error.WriteLine("[selftest] FAIL decode"); return 2; }
    bool ok = got.Kind == msg.Kind && got.Name == msg.Name && got.Data.AsSpan().SequenceEqual(data);
    Console.WriteLine($"[selftest] {(ok ? "PASS" : "FAIL")}: {blob.Length}B in {sw.ElapsedMilliseconds}ms, name='{got.Name}' kind={got.Kind}");
    return ok ? 0 : 3;
}

static async Task<int> Send(string[] a)
{
    var f = Flags(a, out var pos);
    var (host, port) = ParseHost(pos.Count > 0 ? pos[0] : "127.0.0.1:7787");
    PipeMessage msg;
    if (f.TryGetValue("file", out var path)) msg = PipeMessage.FromFile(path);
    else if (f.TryGetValue("text", out var text)) msg = new PipeMessage(PipeKind.Text, f.GetValueOrDefault("name", ""), Encoding.UTF8.GetBytes(text));
    else { Console.Error.WriteLine("send: need --file or --text"); return 1; }
    if (f.TryGetValue("name", out var nm) && msg.Name == "") msg = msg with { Name = nm };

    var opt = new PipeOptions { SendRatePerSecond = f.TryGetValue("rate", out var rr) ? int.Parse(rr) : 200 };
    if (f.TryGetValue("tag", out var tg)) opt.SenderTag = byte.Parse(tg);

    await using var cli = MakeClient(host, port, f.GetValueOrDefault("as", "TPsend"));
    using var ch = new TerrariaClientChannel(cli, ParseCarrier(f));
    using var ses = new PipeSession(ch.Channel, opt);
    ses.OnLog += s => Console.WriteLine("[pipe] " + s);
    ses.OnProgress += p => { if (p.Sending) Console.Write($"\r[send] {p.Done}/{p.Total} chunks ({p.Fraction:P0})   "); };
    Console.WriteLine($"[send] connecting to {host}:{port}...");
    await cli.ConnectAndSpawnAsync(TimeSpan.FromSeconds(30));
    Console.WriteLine($"[send] spawned index {cli.MyIndex}; sending {msg.Kind} '{msg.Name}' ({msg.Data.Length} B)");
    var sw = Stopwatch.StartNew();
    await ses.SendAsync(msg.Encode());
    Console.WriteLine($"\n[send] done in {sw.Elapsed.TotalSeconds:0.0}s");
    return 0;
}

static async Task<int> Recv(string[] a)
{
    var f = Flags(a, out var pos);
    var (host, port) = ParseHost(pos.Count > 0 ? pos[0] : "127.0.0.1:7787");
    string outDir = f.GetValueOrDefault("out", ".");
    bool once = f.ContainsKey("once");
    Directory.CreateDirectory(outDir);

    await using var cli = MakeClient(host, port, f.GetValueOrDefault("as", "TPrecv"));
    using var ch = new TerrariaClientChannel(cli, ParseCarrier(f));
    using var ses = new PipeSession(ch.Channel);
    ses.OnLog += s => Console.WriteLine("[pipe] " + s);
    ses.OnProgress += p => { if (!p.Sending) Console.Write($"\r[recv] {p.Done}/{p.Total} chunks ({p.Fraction:P0})   "); };
    Console.WriteLine($"[recv] connecting to {host}:{port}...");
    await cli.ConnectAndSpawnAsync(TimeSpan.FromSeconds(30));
    Console.WriteLine($"[recv] spawned index {cli.MyIndex}; waiting for transfers (Ctrl+C to stop)");

    var done = new TaskCompletionSource();
    ses.OnBlobReceived += blob =>
    {
        Console.WriteLine();
        if (!PipeMessage.TryDecode(blob, out var msg)) { Console.WriteLine($"[recv] raw blob {blob.Length} B (not a PipeMessage)"); return; }
        switch (msg.Kind)
        {
            case PipeKind.Text:
                Console.WriteLine($"[recv] TEXT: {Encoding.UTF8.GetString(msg.Data)}");
                break;
            default:
                var name = string.IsNullOrEmpty(msg.Name) ? $"recv-{DateTime.Now:HHmmss}.bin" : msg.Name;
                var dest = Path.Combine(outDir, Path.GetFileName(name));
                File.WriteAllBytes(dest, msg.Data);
                Console.WriteLine($"[recv] {msg.Kind} '{msg.Name}' ({msg.Data.Length} B) -> {dest}");
                break;
        }
        if (once) done.TrySetResult();
    };

    using var sigint = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; sigint.Cancel(); };
    await Task.WhenAny(done.Task, cli.WaitForDisconnectAsync(), Task.Delay(Timeout.Infinite, sigint.Token));
    return 0;
}

static async Task<int> LiveTest(string[] a)
{
    var f = Flags(a, out var pos);
    var (host, port) = ParseHost(pos.Count > 0 ? pos[0] : "127.0.0.1:7777");
    string txName = f.GetValueOrDefault("txname", "Isabel");
    string txPass = f.GetValueOrDefault("txpass", "");
    string rxName = f.GetValueOrDefault("rxname", "Victoria");
    string rxPass = f.GetValueOrDefault("rxpass", "");
    int rate = f.TryGetValue("rate", out var rr) ? int.Parse(rr) : 200;

    PipeMessage msg;
    if (f.TryGetValue("file", out var path)) msg = PipeMessage.FromFile(path);
    else { var d = new byte[8000]; for (int i = 0; i < d.Length; i++) d[i] = (byte)(i * 7 + 3); msg = new PipeMessage(PipeKind.Image, "livetest.bin", d); }

    Console.WriteLine($"[live] server {host}:{port}  sender={txName}  receiver={rxName}");

    await using var rx = MakeClient(host, port, rxName);
    await using var tx = MakeClient(host, port, txName);
    rx.OnChat += c => { if (c.AuthorIndex == 255) Console.WriteLine($"[{rxName}<-srv] {c.PlainText}"); };
    tx.OnChat += c => { if (c.AuthorIndex == 255) Console.WriteLine($"[{txName}<-srv] {c.PlainText}"); };

    var carrier = ParseCarrier(f);
    using var rxCh = new TerrariaClientChannel(rx, carrier);
    using var txCh = new TerrariaClientChannel(tx, carrier);
    using var rxSes = new PipeSession(rxCh.Channel);
    using var txSes = new PipeSession(txCh.Channel, new PipeOptions { SendRatePerSecond = rate });
    txSes.OnLog += s => Console.WriteLine("[tx] " + s);
    rxSes.OnLog += s => Console.WriteLine("[rx] " + s);

    Console.WriteLine("[live] connecting receiver...");
    await rx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    Console.WriteLine($"[live] {rxName} spawned (slot {rx.MyIndex}) in '{rx.World.Name}' {rx.World.MaxTilesX}x{rx.World.MaxTilesY} spawn ({rx.World.SpawnTileX},{rx.World.SpawnTileY}) SSC={rx.ServerSideCharacter}");
    Console.WriteLine("[live] connecting sender...");
    await tx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    Console.WriteLine($"[live] {txName} spawned (slot {tx.MyIndex})");

    if (txPass != "") { tx.SendChat($"/login {txPass}"); }
    if (rxPass != "") { rx.SendChat($"/login {rxPass}"); }
    await Task.Delay(3000);

    int w = Math.Max(rx.World.MaxTilesX, 1000);
    int surf = rx.World.WorldSurface > 0 ? rx.World.WorldSurface - 5 : rx.World.SpawnTileY;
    int sep = f.TryGetValue("sep", out var sv) ? int.Parse(sv) : w / 3;
    int txX = Math.Clamp(rx.World.SpawnTileX - sep / 2, 80, w - 80);
    int rxX = Math.Clamp(rx.World.SpawnTileX + sep / 2, 80, w - 80);
    Teleport(tx, txX, surf);
    Teleport(rx, rxX, surf);
    await Task.Delay(1500);
    Console.WriteLine($"[live] positions -> {txName}=tile({tx.Me.TileX},{tx.Me.TileY})  {rxName}=tile({rx.Me.TileX},{rx.Me.TileY})  dX={Math.Abs(tx.Me.TileX - rx.Me.TileX)} tiles");
    if (Math.Abs(tx.Me.TileX - rx.Me.TileX) < 50)
        Console.WriteLine("[live] WARNING: the server kept them close (SSC/anti-cheat may have repositioned); transfer still proceeds (carrier is server-wide).");

    var recvTask = rxSes.ReceiveAsync();
    var sw = Stopwatch.StartNew();
    Console.WriteLine($"[live] sending {msg.Kind} '{msg.Name}' ({msg.Data.Length} B) at {rate} frames/s ...");
    var sendTask = txSes.SendAsync(msg.Encode());
    var finished = await Task.WhenAny(Task.WhenAll(sendTask, recvTask), Task.Delay(TimeSpan.FromMinutes(3)));
    sw.Stop();
    if (finished is Task done && done.IsFaulted) { Console.Error.WriteLine("[live] FAIL: " + done.Exception?.GetBaseException().Message); return 2; }
    if (!recvTask.IsCompletedSuccessfully) { Console.Error.WriteLine("[live] FAIL: receiver did not complete (timeout)"); return 3; }

    var blob = recvTask.Result;
    bool ok = PipeMessage.TryDecode(blob, out var got) && got.Data.AsSpan().SequenceEqual(msg.Data) && got.Name == msg.Name;
    double kbs = msg.Data.Length / 1024.0 / Math.Max(0.001, sw.Elapsed.TotalSeconds);
    int pktsPerFrame = carrier == CarrierKind.Ping ? 4 : 1;
    Console.WriteLine($"[live] RESULT: {(ok ? "PASS" : "FAIL")}  carrier={carrier} {blob.Length} B in {sw.Elapsed.TotalSeconds:0.00}s = {kbs:0.00} KB/s  (frames/s={rate}, ~{rate * pktsPerFrame} pkts/s)  across {Math.Abs(tx.Me.TileX - rx.Me.TileX)} tiles, crc/name {(ok ? "match" : "MISMATCH")}");
    return ok ? 0 : 4;
}

static void Teleport(TerrariaClient c, int tileX, int tileY)
{
    c.Physics.ForcePosition(new Vector2(tileX * 16 + 8 - Player.Width / 2f, tileY * 16 - Player.Height));
    c.SendPlayerControls();
    c.RequestSection(tileX, tileY);
}

static async Task<int> Scan(string[] a)
{
    var f = Flags(a, out var posArg);
    var (host, port) = ParseHost(posArg.Count > 0 ? posArg[0] : "127.0.0.1:7777");
    string txName = f.GetValueOrDefault("txname", "Isabel");
    string txPass = f.GetValueOrDefault("txpass", "");
    string rxName = f.GetValueOrDefault("rxname", "Victoria");
    string rxPass = f.GetValueOrDefault("rxpass", "");

    await using var rx = MakeClient(host, port, rxName);
    await using var tx = MakeClient(host, port, txName);

    var seen = new HashSet<string>();
    rx.OnRawPacket += (type, payload) =>
    {
        var p = payload.Span;
        if (type == 82 && p.Length >= 2)
        {
            ushort mod = (ushort)(p[0] | p[1] << 8);
            if (mod == 2 && p.Length >= 10)
            {
                float x = BitConverter.ToSingle(p.Slice(2, 4)); float y = BitConverter.ToSingle(p.Slice(6, 4));
                if (Math.Abs(x - 16001.5f) < 1 && Math.Abs(y - 8003.5f) < 1) lock (seen) seen.Add("ping(module2)");
            }
            if (mod == 9) lock (seen) seen.Add("particles(module9)");
        }
        else if (type == 120 && p.Length >= 2)
        {
            if (p[1] == 7) lock (seen) seen.Add("emoji(120)");
        }
        else if (type == 91) lock (seen) seen.Add("emotebubble(91)");
    };

    Console.WriteLine($"[scan] connecting {rxName}...");
    await rx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    Console.WriteLine($"[scan] connecting {txName}...");
    await tx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    if (txPass != "") tx.SendChat($"/login {txPass}");
    if (rxPass != "") rx.SendChat($"/login {rxPass}");
    await Task.Delay(3000);
    Console.WriteLine($"[scan] both in; tx slot {tx.MyIndex}, rx slot {rx.MyIndex}. sending candidates...");

    var pingPos = new Vector2(16001.5f, 8003.5f);
    for (int round = 0; round < 6; round++)
    {
        tx.Send(TerrariaHeadless.Protocol.ClientPackets.MapPing(pingPos));
        tx.Send(TerrariaHeadless.Protocol.ClientPackets.Emoji(tx.MyIndex, 7));
        txCh_SendParticle(tx);
        await Task.Delay(700);
    }
    await Task.Delay(1500);

    Console.WriteLine("[scan] RESULTS — relayed to the other client:");
    foreach (var c in new[] { "ping(module2)", "emoji(120)", "emotebubble(91)", "particles(module9)" })
        Console.WriteLine($"    {c,-20} {(seen.Contains(c) ? "YES (relayed)" : "no")}");
    return 0;
}

static void txCh_SendParticle(TerrariaClient tx)
{
    var ch = new ParticleChannel(tx.Send);
    var frame = new byte[ParticleChannel.Payload];
    frame[0] = 0xAB; frame[1] = 0xCD;
    ch.SendFrame(frame);
}

static async Task<int> ScanDist(string[] a)
{
    var f = Flags(a, out var posArg);
    var (host, port) = ParseHost(posArg.Count > 0 ? posArg[0] : "127.0.0.1:7777");
    string txName = f.GetValueOrDefault("txname", "Isabel");
    string txPass = f.GetValueOrDefault("txpass", "");
    string rxName = f.GetValueOrDefault("rxname", "Victoria");
    string rxPass = f.GetValueOrDefault("rxpass", "");
    int sep = f.TryGetValue("sep", out var sv) ? int.Parse(sv) : 2400;

    await using var rx = MakeClient(host, port, rxName);
    await using var tx = MakeClient(host, port, txName);

    var seen = new HashSet<string>();
    int txIndex = -1;
    rx.OnRawPacket += (type, payload) =>
    {
        var p = payload.Span;
        if (type == 82 && p.Length >= 10 && (p[0] | p[1] << 8) == 2)
        {
            float x = BitConverter.ToSingle(p.Slice(2, 4));
            if (Math.Abs(x - 16001.5f) < 1) lock (seen) seen.Add("ping(module2)");
        }
        else if (type == 27 && p.Length >= 4)
        {
            uint key = (uint)(p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24);
            int spawner = (int)(key & 0xFF);
            if (txIndex >= 0 && spawner == txIndex) lock (seen) seen.Add("projectile(27)");
        }
        else if (type == 21) lock (seen) seen.Add("item(21)");
    };

    Console.WriteLine($"[scandist] connecting {rxName}...");
    await rx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    Console.WriteLine($"[scandist] connecting {txName}...");
    await tx.ConnectAndSpawnAsync(TimeSpan.FromSeconds(40));
    if (txPass != "") tx.SendChat($"/login {txPass}");
    if (rxPass != "") rx.SendChat($"/login {rxPass}");
    await Task.Delay(3000);
    txIndex = tx.MyIndex;

    int w = Math.Max(rx.World.MaxTilesX, 1000);
    int surf = rx.World.WorldSurface > 0 ? rx.World.WorldSurface - 5 : rx.World.SpawnTileY;
    int txX = Math.Clamp(rx.World.SpawnTileX - sep / 2, 80, w - 80);
    int rxX = Math.Clamp(rx.World.SpawnTileX + sep / 2, 80, w - 80);
    Teleport(tx, txX, surf);
    Teleport(rx, rxX, surf);
    await Task.Delay(1500);
    Console.WriteLine($"[scandist] tx tile({tx.Me.TileX},{tx.Me.TileY}) rx tile({rx.Me.TileX},{rx.Me.TileY}) dX={Math.Abs(tx.Me.TileX-rx.Me.TileX)}; sending candidates...");

    var pingPos = new Vector2(16001.5f, 8003.5f);
    uint projKey = (uint)((tx.MyIndex & 0xFF) | ((900 & 0x3FF) << 8));
    float px = tx.Me.Position.X, py = tx.Me.Position.Y;
    for (int round = 0; round < 8; round++)
    {
        tx.Send(TerrariaHeadless.Protocol.ClientPackets.MapPing(pingPos));
        var pw = new TerrariaHeadless.Protocol.PacketWriter(TerrariaHeadless.Protocol.MessageID.SyncProjectile)
            .Int((int)projKey).Float(px).Float(py).Float(0.01f * round).Float(0f).Short(1020).Byte(0);
        tx.Send(pw.ToArray());
        await Task.Delay(600);
    }
    await Task.Delay(1500);

    Console.WriteLine("[scandist] RESULTS at distance:");
    foreach (var c in new[] { "ping(module2)", "projectile(27)", "item(21)" })
        Console.WriteLine($"    {c,-18} {(seen.Contains(c) ? "YES" : "no")}");
    return 0;
}

sealed class RawLoopback
{
    private readonly List<IReceivingCarrier> _eps = new();
    private readonly object _sync = new();
    private readonly Random _rng = new(1234);
    public double LossRate;
    public int MaxLatencyMs;

    public ITerraChannel CreateCarrier(CarrierKind kind, int spawner)
    {
        var self = new IReceivingCarrier[1];
        Action<byte[]> sendRaw = pkt => Relay(self[0], pkt);
        ITerraChannel ch = kind switch
        {
            CarrierKind.Ping => new PingChannel(sendRaw),
            _ => new ParticleChannel(sendRaw),
        };
        self[0] = (IReceivingCarrier)ch;
        lock (_sync) _eps.Add(self[0]);
        return ch;
    }

    private void Relay(IReceivingCarrier from, byte[] pkt)
    {
        byte type = pkt[2];
        var payload = pkt[3..];
        List<IReceivingCarrier> others;
        lock (_sync) others = _eps.Where(e => e != from).ToList();
        foreach (var e in others)
        {
            double loss; int delay;
            lock (_sync) { loss = _rng.NextDouble(); delay = MaxLatencyMs > 0 ? _rng.Next(MaxLatencyMs) : 0; }
            if (loss < LossRate) continue;
            if (delay > 0) Task.Delay(delay).ContinueWith(_ => e.Feed(type, payload));
            else e.Feed(type, payload);
        }
    }
}
