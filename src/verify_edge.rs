//! Standalone check: loads an already-synced Edge Shard directory and runs a
//! query against it, so you can confirm a GUI sync actually worked.
//!
//! Usage:
//!   cargo run --bin verify_edge -- ./edge-shard my-vector 0.1 0.2 0.3 0.4
//!
//! Run this from the *same* directory (or pass the same absolute path) that
//! you gave the GUI as the "Local Edge Shard directory" — EdgeShard::load
//! opens an existing shard, it does not create one.

use std::path::Path;

use qdrant_edge::*;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);

    let dir = args.next().unwrap_or_else(|| "./edge-shard".to_string());
    let vector_name = args.next().unwrap_or_else(|| "my-vector".to_string());
    let query_vec: Vec<f32> = args
        .map(|s| s.parse().expect("query components must be floats"))
        .collect();

    let path = Path::new(&dir);
    if !path.exists() {
        anyhow::bail!(
            "'{}' doesn't exist yet — run the GUI sync first (or check you're \
             pointing at the same directory it wrote to).",
            path.display()
        );
    }

    println!("Loading Edge Shard from {}…", path.display());
    let shard = EdgeShard::load(path, None)?;

    let query_vec = if query_vec.is_empty() {
        vec![0.1, 0.2, 0.3, 0.4]
    } else {
        query_vec
    };
    println!("Querying '{vector_name}' with {query_vec:?}…");

    let results = shard.query(
        QueryRequestBuilder::new(5)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                query: query_vec.into(),
                using: Some(vector_name),
            })))
            .with_payload(WithPayloadInterface::Bool(true))
            .build(),
    )?;

    println!("\n{} point(s) returned:", results.len());
    for point in results {
        println!(
            "  id={:?} score={:.4} payload={:?}",
            point.id, point.score, point.payload
        );
    }

    Ok(())
}
