# TerraPipe

TerraPipe moves data between two Terraria clients through the server network.
It does not modify the server. A vanilla or TShock server rebroadcasts certain
packets as is to the other clients. TerraPipe hides inside one of those packets.
It splits the data into chunks with error control and reassembles it on the
other side. It does not use chat. It does not show up in chat logs. It is
invisible on screen.

## Layout

```
TerraPipe/
├── src/TerraPipe.Core        protocol library, no dependencies, net8.0
│   ├── Carriers/             transport channels: ping, particle, loopback
│   └── Protocol/             framing, ARQ session with selective ACK, CRC, self describing messages
└── src/TerraPipe.Cli         terrapipe executable: probe / selftest / send / recv
```

## Carriers

There are two carriers. Both ride on packet 82, a NetModule the server relays
verbatim to the other clients. Pick one with `--carrier`.

- **`ping`** is the most stable and works on most servers. It is slower. Each
  21 byte frame is split across 4 ping packets with a CRC-8 check and
  reassembled on the other side. Nothing is drawn on screen.
- **`particle`** is faster but does not work on many servers. It fits a whole
  frame in a single packet using an out of range particle type, so no client
  draws anything.

On top of the channel, `PipeSession` runs a sliding window ARQ: OPEN → DATA →
ACK → FIN → DONE. It tolerates loss, duplicates and reordering, with a CRC-16
check on the full blob.

## Build

```powershell
cd TerraPipe
dotnet build -c Release
# executable: src/TerraPipe.Cli/bin/Release/net8.0/terrapipe.dll
```

## Commands

```
terrapipe probe    <host:port>
    Connect two clients and confirm the carrier is forwarded.

terrapipe selftest [--loss 0.15] [--reorder 40] [--size N]
    Run the full protocol over an in memory channel with loss and reordering. No server needed.

terrapipe send <host:port> --name N (--file PATH | --text "message") [--rate 200] [--carrier ping|particle]
    Send a file or text.

terrapipe recv <host:port> [--out DIR] [--once] [--carrier ping|particle]
    Listen for transfers. Save data to DIR and print text.
```

Both sides must use the same carrier.

Example against a local TShock server on 127.0.0.1:7787.

```powershell
# terminal 1
dotnet terrapipe.dll recv 127.0.0.1:7787 --out .\received --carrier ping
# terminal 2
dotnet terrapipe.dll send 127.0.0.1:7787 --file .\photo.png --carrier ping
```
