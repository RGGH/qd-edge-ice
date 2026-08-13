use std::path::{Path, PathBuf};

use futures_util::{Stream, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Everything the UI needs to know to sync an *entire collection* — every
/// shard the server reports — down to local Edge Shards. There is no
/// per-shard input anymore: the shard list is discovered from the server.
#[derive(Debug, Clone)]
pub struct CollectionSyncRequest {
    pub server_url: String,
    pub api_key: String,
    pub collection: String,
    /// Parent directory. Each shard gets its own subdirectory underneath,
    /// e.g. `{target_dir}/shard-0`, `{target_dir}/shard-1`, ...
    pub target_dir: String,
}

/// Result of a successful download: where the .snapshot file landed, and its size.
#[derive(Debug, Clone)]
struct DownloadedSnapshot {
    path: PathBuf,
    bytes: u64,
}

/// Events emitted while an entire collection (all shards) syncs to Edge.
/// Mirrors the flow in the sync diagram:
/// `shards -> snapshot (per shard) -> Sync Manager -> EdgeShard N`.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    /// Emitted once, right after the shard list is fetched from the server.
    ShardsDiscovered(Vec<u32>),
    /// A given shard has started downloading/unpacking.
    ShardStarted { shard_id: u32, index: usize, total_shards: usize },
    /// Download progress for the shard currently in flight.
    /// `total` is `None` when the server didn't send a `Content-Length`
    /// header — in that case the UI should treat the bar as indeterminate
    /// rather than claiming a precise percentage.
    Progress { shard_id: u32, downloaded: u64, total: Option<u64> },
    /// A shard finished downloading and was unpacked into its own EdgeShard dir.
    ShardCompleted { shard_id: u32, summary: String },
    /// A shard failed; the sync continues on to the remaining shards.
    ShardFailed { shard_id: u32, error: String },
    /// The whole collection sync has finished. `Err` means at least one
    /// shard failed (see the individual `ShardFailed` events/log for detail),
    /// or the shard list itself couldn't be fetched.
    Done(Result<(), String>),
}

/// Discovers every shard for `collection` and syncs each one to its own
/// local Edge Shard directory under `req.target_dir`, in order.
pub fn sync_all_shards_stream(
    req: CollectionSyncRequest,
) -> impl Stream<Item = SyncEvent> + Send + 'static {
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        run_sync_all(req, tx).await;
    });

    UnboundedReceiverStream::new(rx)
}

async fn run_sync_all(req: CollectionSyncRequest, tx: mpsc::UnboundedSender<SyncEvent>) {
    let shard_ids = match fetch_shard_ids(&req.server_url, &req.api_key, &req.collection).await {
        Ok(ids) => ids,
        Err(err) => {
            let _ = tx.send(SyncEvent::Done(Err(err)));
            return;
        }
    };

    let _ = tx.send(SyncEvent::ShardsDiscovered(shard_ids.clone()));

    let total_shards = shard_ids.len();
    let mut failures: Vec<String> = Vec::new();

    for (index, shard_id) in shard_ids.into_iter().enumerate() {
        let _ = tx.send(SyncEvent::ShardStarted { shard_id, index, total_shards });

        match sync_one_shard(&req, shard_id, &tx).await {
            Ok(summary) => {
                let _ = tx.send(SyncEvent::ShardCompleted { shard_id, summary });
            }
            Err(error) => {
                failures.push(format!("shard {shard_id}: {error}"));
                let _ = tx.send(SyncEvent::ShardFailed { shard_id, error });
            }
        }
    }

    if failures.is_empty() {
        let _ = tx.send(SyncEvent::Done(Ok(())));
    } else {
        let _ = tx.send(SyncEvent::Done(Err(format!(
            "{} of {total_shards} shard(s) failed: {}",
            failures.len(),
            failures.join("; ")
        ))));
    }
}

/// Asks the server which shards `collection` has. Uses the cluster info
/// endpoint (`/collections/{collection}/cluster`), which reports the shards
/// this node holds locally — the ones we can snapshot and pull.
async fn fetch_shard_ids(
    server_url: &str,
    api_key: &str,
    collection: &str,
) -> Result<Vec<u32>, String> {
    let base = server_url.trim_end_matches('/');
    let url = format!("{base}/collections/{collection}/cluster");

    let client = reqwest::Client::new();
    let mut builder = client.get(&url);
    if !api_key.trim().is_empty() {
        builder = builder.header("api-key", api_key.trim());
    }

    let response = builder
        .send()
        .await
        .map_err(|e| format!("Request to {url} failed: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Server returned {status} for {url}: {body}"));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Could not parse cluster info from {url}: {e}"))?;

    let mut shard_ids: Vec<u32> = body["result"]["local_shards"]
        .as_array()
        .map(|shards| {
            shards
                .iter()
                .filter_map(|shard| shard["shard_id"].as_u64())
                .map(|id| id as u32)
                .collect()
        })
        .unwrap_or_default();

    if shard_ids.is_empty() {
        return Err(format!(
            "No local shards reported for collection '{collection}' at {url}"
        ));
    }

    shard_ids.sort_unstable();
    shard_ids.dedup();
    Ok(shard_ids)
}

async fn sync_one_shard(
    req: &CollectionSyncRequest,
    shard_id: u32,
    tx: &mpsc::UnboundedSender<SyncEvent>,
) -> Result<String, String> {
    let snapshot =
        download_shard_snapshot(&req.server_url, &req.api_key, &req.collection, shard_id, tx)
            .await?;
    let downloaded_mb = snapshot.bytes as f64 / (1024.0 * 1024.0);

    let shard_dir = shard_target_dir(&req.target_dir, shard_id);
    let summary = unpack_snapshot_to_edge(snapshot.path, shard_dir).await?;
    Ok(format!("{summary} ({downloaded_mb:.2} MB downloaded)"))
}

/// The local directory a given shard's EdgeShard lives in, e.g.
/// `{target_dir}/shard-0`.
fn shard_target_dir(target_dir: &str, shard_id: u32) -> String {
    Path::new(target_dir)
        .join(format!("shard-{shard_id}"))
        .to_string_lossy()
        .into_owned()
}

async fn download_shard_snapshot(
    server_url: &str,
    api_key: &str,
    collection: &str,
    shard_id: u32,
    tx: &mpsc::UnboundedSender<SyncEvent>,
) -> Result<DownloadedSnapshot, String> {
    let base = server_url.trim_end_matches('/');
    let url = format!("{base}/collections/{collection}/shards/{shard_id}/snapshot");

    let client = reqwest::Client::new();
    let mut builder = client.get(&url);
    if !api_key.trim().is_empty() {
        builder = builder.header("api-key", api_key.trim());
    }

    let response = builder
        .send()
        .await
        .map_err(|e| format!("Request to {url} failed: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Server returned {status} for {url}: {body}"));
    }

    let total = response.content_length();

    let file_name = format!("{collection}-shard-{shard_id}.snapshot");
    let out_path = std::env::temp_dir().join(file_name);

    let mut file = tokio::fs::File::create(&out_path)
        .await
        .map_err(|e| format!("Could not create temp file {}: {e}", out_path.display()))?;

    let mut stream = response.bytes_stream();
    let mut written: u64 = 0;

    // Fire an initial 0% event as soon as headers arrive, so the bar
    // appears right away instead of staying invisible until the first chunk.
    let _ = tx.send(SyncEvent::Progress { shard_id, downloaded: 0, total });

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("Error while downloading snapshot: {e}"))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("Error while writing snapshot to disk: {e}"))?;
        written += chunk.len() as u64;

        let _ = tx.send(SyncEvent::Progress { shard_id, downloaded: written, total });
    }
    file.flush().await.map_err(|e| e.to_string())?;

    Ok(DownloadedSnapshot { path: out_path, bytes: written })
}

/// Unpack a downloaded .snapshot file into a local Qdrant Edge Shard directory,
/// then load it to confirm it's valid.
async fn unpack_snapshot_to_edge(
    snapshot_path: PathBuf,
    target_dir: String,
) -> Result<String, String> {
    tokio::task::spawn_blocking(move || unpack_blocking(&snapshot_path, &target_dir))
        .await
        .map_err(|e| format!("Unpack task panicked: {e}"))?
}

fn unpack_blocking(snapshot_path: &Path, target_dir: &str) -> Result<String, String> {
    let dir = PathBuf::from(target_dir);

    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .map_err(|e| format!("Could not clear existing directory {}: {e}", dir.display()))?;
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Could not create directory {}: {e}", dir.display()))?;

    qdrant_edge::EdgeShard::unpack_snapshot(snapshot_path, &dir)
        .map_err(|e| format!("unpack_snapshot failed: {e}"))?;

    // Load it back to confirm the shard is valid and readable, then drop it
    // immediately so the WAL/segments are flushed and the directory is left
    // in a clean, closed state for whoever opens it next.
    let shard = qdrant_edge::EdgeShard::load(&dir, None)
        .map_err(|e| format!("Snapshot unpacked, but EdgeShard::load failed: {e}"))?;
    drop(shard);

    Ok(format!(
        "Edge Shard ready at {} (from {})",
        dir.display(),
        snapshot_path.display()
    ))
}