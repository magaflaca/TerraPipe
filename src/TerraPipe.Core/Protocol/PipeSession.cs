using System.Buffers.Binary;
using System.Collections;
using TerraPipe.Carriers;

namespace TerraPipe.Protocol;

public sealed class PipeOptions
{
    public int SendRatePerSecond = 200;
    public int WindowChunks = 32;
    public TimeSpan AckInterval = TimeSpan.FromMilliseconds(200);
    public TimeSpan AckMinGap = TimeSpan.FromMilliseconds(20);
    public TimeSpan ControlResend = TimeSpan.FromMilliseconds(500);
    public TimeSpan RetransmitTimeout = TimeSpan.FromMilliseconds(400);
    public TimeSpan Timeout = TimeSpan.FromMinutes(5);
    public byte SenderTag = (byte)Random.Shared.Next(1, 256);
}

public readonly record struct PipeProgress(long Done, long Total, bool Sending)
{
    public double Fraction => Total <= 0 ? 1 : (double)Done / Total;
}

public sealed class PipeSession : IDisposable
{
    private readonly ITerraChannel _ch;
    private readonly int _dataBytes;
    private readonly PipeOptions _opt;
    private readonly object _sync = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _pump;

    private Outgoing? _tx;
    private readonly Dictionary<int, Incoming> _rx = new();

    public event Action<byte[]>? OnBlobReceived;
    public event Action<PipeProgress>? OnProgress;
    public event Action<string>? OnLog;

    public PipeSession(ITerraChannel channel, PipeOptions? options = null)
    {
        _ch = channel;
        _dataBytes = Math.Max(1, _ch.FrameSize - (PipeFrame.HeaderSize + 2));
        _opt = options ?? new PipeOptions();
        _ch.OnFrame += OnFrame;
        _pump = Task.Run(PumpLoop);
    }

    public async Task SendAsync(byte[] blob, CancellationToken ct = default)
    {
        var tx = new Outgoing(blob, _opt, _dataBytes);
        lock (_sync)
        {
            if (_tx is { Done.Task.IsCompleted: false }) throw new InvalidOperationException("a send is already in progress");
            _tx = tx;
        }
        Log($"send start: {blob.Length} B in {tx.ChunkCount} chunks, stream {tx.StreamId}, tag {tx.SenderTag}");
        using var reg = ct.Register(() => tx.Done.TrySetCanceled());
        using var timeout = new CancellationTokenSource(_opt.Timeout);
        using var treg = timeout.Token.Register(() => tx.Done.TrySetException(new TimeoutException("transfer timed out")));
        await tx.Done.Task.ConfigureAwait(false);
        Log("send complete");
    }

    public async Task<byte[]> ReceiveAsync(CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        void H(byte[] b) => tcs.TrySetResult(b);
        OnBlobReceived += H;
        try
        {
            using var reg = ct.Register(() => tcs.TrySetCanceled());
            return await tcs.Task.ConfigureAwait(false);
        }
        finally { OnBlobReceived -= H; }
    }

    private void OnFrame(byte[] f)
    {
        if (f.Length < PipeFrame.HeaderSize) return;
        var type = (FrameType)f[PipeFrame.TypeOffset];
        byte senderTag = f[PipeFrame.SenderOffset];
        byte streamId = f[PipeFrame.StreamOffset];
        var body = f.AsSpan(PipeFrame.BodyOffset);

        switch (type)
        {
            case FrameType.Ack: HandleAck(senderTag, streamId, body); break;
            case FrameType.Done: HandleDone(senderTag, streamId, body); break;
            case FrameType.Open: HandleOpen(senderTag, streamId, body); break;
            case FrameType.Data: HandleData(senderTag, streamId, body); break;
            case FrameType.Fin: HandleFin(senderTag, streamId); break;
            case FrameType.Abort: HandleAbort(senderTag, streamId); break;
        }
    }

    private void HandleAck(byte senderTag, byte streamId, ReadOnlySpan<byte> body)
    {
        lock (_sync)
        {
            if (_tx is not { } tx || tx.SenderTag != senderTag || tx.StreamId != streamId) return;
            if (body.Length < 6) return;
            int baseSeq = BinaryPrimitives.ReadUInt16LittleEndian(body);
            uint bitmap = BinaryPrimitives.ReadUInt32LittleEndian(body[2..]);
            tx.ApplyAck(baseSeq, bitmap);
        }
    }

    private void HandleDone(byte senderTag, byte streamId, ReadOnlySpan<byte> body)
    {
        bool ok = body.Length >= 1 && body[0] == 1;
        lock (_sync)
        {
            if (_tx is not { } tx || tx.SenderTag != senderTag || tx.StreamId != streamId) return;
            if (ok) tx.Done.TrySetResult(true);
            else tx.Done.TrySetException(new IOException("receiver reported CRC mismatch"));
        }
    }

    private void HandleOpen(byte senderTag, byte streamId, ReadOnlySpan<byte> body)
    {
        if (body.Length < 8) return;
        long totalLen = BinaryPrimitives.ReadUInt32LittleEndian(body);
        int chunks = BinaryPrimitives.ReadUInt16LittleEndian(body[4..]);
        ushort crc = BinaryPrimitives.ReadUInt16LittleEndian(body[6..]);
        int key = senderTag << 8 | streamId;
        lock (_sync)
        {
            if (_rx.TryGetValue(key, out var ex) && ex.ChunkCount == chunks && ex.Crc == crc) return;
            _rx[key] = new Incoming(senderTag, streamId, totalLen, chunks, crc, _dataBytes);
            Log($"recv open: {totalLen} B in {chunks} chunks from tag {senderTag}/{streamId}");
        }
    }

    private void HandleData(byte senderTag, byte streamId, ReadOnlySpan<byte> body)
    {
        if (body.Length < 2) return;
        int seq = BinaryPrimitives.ReadUInt16LittleEndian(body);
        var data = body[2..];
        int key = senderTag << 8 | streamId;
        Incoming? inc;
        lock (_sync) { _rx.TryGetValue(key, out inc); }
        if (inc == null) return;
        bool complete;
        lock (inc.Sync) complete = inc.Store(seq, data);
        if (inc.Received % 64 == 0 || complete) OnProgress?.Invoke(new PipeProgress(inc.Received, inc.ChunkCount, false));
        if (complete) { FinishIncoming(key, inc); return; }
        var now = DateTime.UtcNow;
        if (now >= inc.NextFastAckAt) { SendAck(inc); inc.NextFastAckAt = now + _opt.AckMinGap; inc.NextAckAt = now + _opt.AckInterval; }
    }

    private void HandleFin(byte senderTag, byte streamId)
    {
        int key = senderTag << 8 | streamId;
        Incoming? inc;
        lock (_sync) { _rx.TryGetValue(key, out inc); }
        if (inc == null) return;
        inc.FinSeen = true;
        if (inc.Completed)
        {
            SendDone(inc, inc.CrcOk);
            inc.DropAt = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        }
        else SendAck(inc);
    }

    private void HandleAbort(byte senderTag, byte streamId)
    {
        int key = senderTag << 8 | streamId;
        lock (_sync)
        {
            _rx.Remove(key);
            if (_tx is { } tx && tx.SenderTag == senderTag && tx.StreamId == streamId)
                tx.Done.TrySetException(new IOException("peer aborted"));
        }
    }

    private void FinishIncoming(int key, Incoming inc)
    {
        byte[] blob;
        bool ok;
        lock (inc.Sync)
        {
            if (inc.Completed) { SendDone(inc, inc.CrcOk); return; }
            blob = inc.Buffer;
            ok = Crc16.Compute(blob) == inc.Crc;
            inc.Completed = true;
            inc.CrcOk = ok;
        }
        Log($"recv complete: {blob.Length} B, crc {(ok ? "ok" : "MISMATCH")}");
        SendDone(inc, ok);
        OnProgress?.Invoke(new PipeProgress(inc.ChunkCount, inc.ChunkCount, false));
        if (ok) OnBlobReceived?.Invoke(blob);
        inc.DropAt = DateTime.UtcNow + TimeSpan.FromSeconds(5);
    }

    private async Task PumpLoop()
    {
        var last = DateTime.UtcNow;
        double budget = 0;
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var now = DateTime.UtcNow;
                double elapsed = (now - last).TotalSeconds; last = now;
                budget = Math.Min(budget + elapsed * _opt.SendRatePerSecond, _opt.WindowChunks);

                Outgoing? tx;
                lock (_sync) tx = _tx is { Done.Task.IsCompleted: false } ? _tx : null;
                if (tx != null)
                {
                    if (!tx.OpenAcked && now >= tx.NextControlAt)
                    {
                        SendOpen(tx);
                        tx.NextControlAt = now + _opt.ControlResend;
                    }
                    if (tx.OpenAcked)
                    {
                        int seq;
                        while (budget >= 1 && (seq = tx.NextFresh()) >= 0) { SendData(tx, seq); budget -= 1; }

                        if (tx.NextFresh() < 0)
                        {
                            if (tx.AckedCount < tx.ChunkCount && now >= tx.NextRetransmitAt)
                            {
                                foreach (int s in tx.Unacked())
                                {
                                    if (budget < 1) break;
                                    SendData(tx, s); budget -= 1;
                                }
                                tx.NextRetransmitAt = now + _opt.RetransmitTimeout;
                            }
                            if (now >= tx.NextControlAt) { SendFin(tx); tx.NextControlAt = now + _opt.ControlResend; }
                        }
                        OnProgress?.Invoke(new PipeProgress(tx.AckedCount, tx.ChunkCount, true));
                    }
                }

                List<Incoming> incs;
                lock (_sync) incs = _rx.Values.ToList();
                foreach (var inc in incs)
                {
                    if (!inc.Completed && now >= inc.NextAckAt) { SendAck(inc); inc.NextAckAt = now + _opt.AckInterval; }
                    if (inc.DropAt is { } d && now >= d) lock (_sync) _rx.Remove(inc.Key);
                }

                await Task.Delay(5, _cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
    }

    private void SendOpen(Outgoing tx)
    {
        Span<byte> f = stackalloc byte[PipeFrame.HeaderSize + 8];
        f[0] = (byte)FrameType.Open; f[1] = tx.SenderTag; f[2] = tx.StreamId;
        BinaryPrimitives.WriteUInt32LittleEndian(f[3..], (uint)tx.Blob.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(f[7..], (ushort)tx.ChunkCount);
        BinaryPrimitives.WriteUInt16LittleEndian(f[9..], tx.Crc);
        _ch.SendFrame(f);
    }

    private void SendData(Outgoing tx, int seq)
    {
        int off = seq * _dataBytes;
        int n = Math.Min(_dataBytes, tx.Blob.Length - off);
        Span<byte> f = stackalloc byte[PipeFrame.HeaderSize + 2 + _dataBytes];
        f[0] = (byte)FrameType.Data; f[1] = tx.SenderTag; f[2] = tx.StreamId;
        BinaryPrimitives.WriteUInt16LittleEndian(f[3..], (ushort)seq);
        tx.Blob.AsSpan(off, n).CopyTo(f[5..]);
        _ch.SendFrame(f[..(5 + n)]);
        tx.MarkSent(seq);
    }

    private void SendFin(Outgoing tx)
    {
        Span<byte> f = stackalloc byte[PipeFrame.HeaderSize];
        f[0] = (byte)FrameType.Fin; f[1] = tx.SenderTag; f[2] = tx.StreamId;
        _ch.SendFrame(f);
    }

    private void SendAck(Incoming inc)
    {
        int baseSeq; uint bitmap;
        lock (inc.Sync) (baseSeq, bitmap) = inc.AckState();
        Span<byte> f = stackalloc byte[PipeFrame.HeaderSize + 6];
        f[0] = (byte)FrameType.Ack; f[1] = inc.SenderTag; f[2] = inc.StreamId;
        BinaryPrimitives.WriteUInt16LittleEndian(f[3..], (ushort)baseSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(f[5..], bitmap);
        _ch.SendFrame(f);
    }

    private void SendDone(Incoming inc, bool ok)
    {
        Span<byte> f = stackalloc byte[PipeFrame.HeaderSize + 1];
        f[0] = (byte)FrameType.Done; f[1] = inc.SenderTag; f[2] = inc.StreamId; f[3] = (byte)(ok ? 1 : 0);
        _ch.SendFrame(f);
    }

    private void Log(string s) => OnLog?.Invoke(s);

    public void Dispose()
    {
        _ch.OnFrame -= OnFrame;
        _cts.Cancel();
        try { _pump.Wait(500); } catch { }
        _cts.Dispose();
    }

    private sealed class Outgoing
    {
        public readonly byte[] Blob;
        public readonly int ChunkCount;
        public readonly ushort Crc;
        public readonly byte SenderTag;
        public readonly byte StreamId = (byte)Random.Shared.Next(1, 256);
        public readonly TaskCompletionSource<bool> Done = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly BitArray _acked;
        private readonly BitArray _sent;
        private readonly int _window;
        public int AckedCount { get; private set; }
        public bool OpenAcked { get; private set; }
        public DateTime NextControlAt = DateTime.UtcNow;
        public DateTime NextRetransmitAt = DateTime.UtcNow;

        public Outgoing(byte[] blob, PipeOptions opt, int dataBytes)
        {
            Blob = blob;
            ChunkCount = Math.Max(1, (blob.Length + dataBytes - 1) / dataBytes);
            Crc = Crc16.Compute(blob);
            SenderTag = opt.SenderTag;
            _acked = new BitArray(ChunkCount);
            _sent = new BitArray(ChunkCount);
            _window = opt.WindowChunks;
        }

        public void ApplyAck(int baseSeq, uint bitmap)
        {
            OpenAcked = true;
            for (int i = 0; i < baseSeq && i < ChunkCount; i++) Set(i);
            for (int b = 0; b < 32; b++)
                if ((bitmap & 1u << b) != 0) { int s = baseSeq + b; if (s < ChunkCount) Set(s); }
        }

        private void Set(int i) { if (!_acked[i]) { _acked[i] = true; AckedCount++; } }

        public int NextFresh()
        {
            int frontier = 0; while (frontier < ChunkCount && _acked[frontier]) frontier++;
            int limit = Math.Min(ChunkCount, frontier + _window);
            for (int i = frontier; i < limit; i++) if (!_acked[i] && !_sent[i]) return i;
            return -1;
        }

        public IEnumerable<int> Unacked()
        {
            for (int i = 0; i < ChunkCount; i++) if (!_acked[i] && _sent[i]) yield return i;
        }

        public void MarkSent(int seq) => _sent[seq] = true;
    }

    private sealed class Incoming
    {
        public readonly object Sync = new();
        public readonly byte SenderTag, StreamId;
        public readonly int ChunkCount;
        public readonly ushort Crc;
        public readonly byte[] Buffer;
        private readonly int _dataBytes;
        private readonly BitArray _have;
        public int Received { get; private set; }
        public bool FinSeen, Completed, CrcOk;
        public DateTime NextAckAt = DateTime.UtcNow;
        public DateTime NextFastAckAt = DateTime.UtcNow;
        public DateTime? DropAt;
        public int Key => SenderTag << 8 | StreamId;

        public Incoming(byte senderTag, byte streamId, long totalLen, int chunks, ushort crc, int dataBytes)
        {
            SenderTag = senderTag; StreamId = streamId; ChunkCount = chunks; Crc = crc; _dataBytes = dataBytes;
            Buffer = new byte[totalLen];
            _have = new BitArray(chunks);
        }

        public bool Store(int seq, ReadOnlySpan<byte> data)
        {
            if (seq < 0 || seq >= ChunkCount || _have[seq]) return Received == ChunkCount;
            int off = seq * _dataBytes;
            int n = Math.Min(data.Length, Buffer.Length - off);
            data[..n].CopyTo(Buffer.AsSpan(off));
            _have[seq] = true; Received++;
            return Received == ChunkCount;
        }

        public (int, uint) AckState()
        {
            int baseSeq = 0; while (baseSeq < ChunkCount && _have[baseSeq]) baseSeq++;
            uint bitmap = 0;
            for (int b = 0; b < 32; b++) { int s = baseSeq + b; if (s < ChunkCount && _have[s]) bitmap |= 1u << b; }
            return (baseSeq, bitmap);
        }
    }
}
