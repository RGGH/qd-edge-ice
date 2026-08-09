# Qdrant Edge Snapshotter

<img width="1280" height="720" alt="iced-rs" src="https://github.com/user-attachments/assets/9dee77b8-1cb2-49cb-9dbd-102bed295a5e" />


A small iced (0.14) desktop GUI that does what the Qdrant Edge docs describe by hand:

1. **Download** a shard snapshot from a running Qdrant **server**
   (`GET /collections/{collection}/shards/{shard}/snapshot`)
2. **Unpack** it into a local **Qdrant Edge Shard** directory
   (`EdgeShard::unpack_snapshot` + `EdgeShard::load`)

This follows the pattern from:
- https://qdrant.tech/documentation/edge/edge-data-synchronization-patterns/#initialize-edge-shard-from-existing-qdrant-collection
- https://qdrant.tech/documentation/edge/edge-synchronization-guide/

Important: `EdgeShard::load` opens an **existing** shard, it does not create
one. Point it at the exact same directory (same working directory or same
absolute path) that you gave the GUI's "Local Edge Shard directory" field,
and only after the GUI's log panel showed a `✓` success line. If you get a
"failed to create WAL directory" error, that almost always means the GUI
sync hasn't run (successfully) against that path yet — not a bug in the
verifier itself.

## Build & run

```bash
rustup update stable   # make sure you're on a recent toolchain
cd qdrant-edge-ice
cargo run
```

```bash
cargo r
```

First build will take a while — it pulls in `wgpu` (iced's renderer).

## What it does

- Form fields: server URL, API key, collection name, shard ID, and a local
  target directory (with a native folder picker via `rfd`).
- "Download snapshot & sync to Edge" button:
  - streams the snapshot from the server to a temp file (`reqwest`, async,
    doesn't block the UI thread)
  - wipes/recreates the target directory, then calls
    `qdrant_edge::EdgeShard::unpack_snapshot` and `EdgeShard::load` inside
    `tokio::task::spawn_blocking` (this is blocking file I/O)
- A scrolling log panel shows progress/errors.

## Known simplifications / good next steps

- **No progress bar for the download itself.** `Task::perform` only reports
  one final value; a byte-level progress bar needs `iced::Subscription` or
  `Task::stream` piping chunk counts back as messages. Happy to add this.
- **Full snapshot only.** The docs also describe *partial* snapshots
  (`snapshot_manifest()` + `POST .../snapshot/partial/create` +
  `update_from_snapshot`) for cheap incremental re-syncs — worth adding as a
  "Re-sync" button once you have this compiling, so you're not re-downloading
  the whole shard every time.
- **Single shard only.** If your collection has more than one shard, you'd
  want a dropdown to list/select shard IDs rather than a free-text field.
- **No dual-write / mutable shard side.** This tool only covers
  server → Edge. The full pattern in the docs also has a *mutable* local
  shard for offline writes that get queued back up to the server — out of
  scope for a "download a snapshot" GUI, but the `edge_sync.rs` module is a
  reasonable place to grow that into.

## Files

- `Cargo.toml`
- `src/main.rs` — iced UI (state, messages, view)
- `src/edge_sync.rs` — snapshot download + unpack logic, independent of the UI


