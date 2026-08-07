use std::path::{Path, PathBuf};

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

/// Everything the UI needs to know to build a snapshot request.
#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    pub server_url: String,
    pub api_key: String,
    pub collection: String,
    pub shard_id: String,
    pub target_dir: String,
}

/// Result of a successful download: where the .snapshot file landed, and its size.
#[derive(Debug, Clone)]
pub struct DownloadedSnapshot {
    pub path: PathBuf,
    pub bytes: u64,
}

/// GET /collections/{collection}/shards/{shard}/snapshot from the server and
/// stream it to a temp file on disk.
///
/// Mirrors the pattern in the Qdrant Edge docs:
/// https://qdrant.tech/documentation/edge/edge-data-synchronization-patterns/
pub async fn download_snapshot(req: SnapshotRequest) -> Result<DownloadedSnapshot, String> {
    let base = req.server_url.trim_end_matches('/');
    let url = format!(
        "{base}/collections/{}/shards/{}/snapshot",
        req.collection, req.shard_id
    );

    let client = reqwest::Client::new();
    let mut builder = client.get(&url);
    if !req.api_key.trim().is_empty() {
        builder = builder.header("api-key", req.api_key.trim());
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

    let file_name = format!("{}-shard-{}.snapshot", req.collection, req.shard_id);
    let out_path = std::env::temp_dir().join(file_name);

    let mut file = tokio::fs::File::create(&out_path)
        .await
        .map_err(|e| format!("Could not create temp file {}: {e}", out_path.display()))?;

    let mut stream = response.bytes_stream();
    let mut written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("Error while downloading snapshot: {e}"))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("Error while writing snapshot to disk: {e}"))?;
        written += chunk.len() as u64;
    }
    file.flush().await.map_err(|e| e.to_string())?;

    Ok(DownloadedSnapshot {
        path: out_path,
        bytes: written,
    })
}

/// Unpack a downloaded .snapshot file into a local Qdrant Edge Shard directory,
/// then load it to confirm it's valid. This replaces `target_dir` entirely, as
/// `EdgeShard::unpack_snapshot` / `EdgeShard::new` expect an empty directory
/// (see the Edge Quickstart and Synchronization Guide).
pub async fn unpack_snapshot_to_edge(
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
