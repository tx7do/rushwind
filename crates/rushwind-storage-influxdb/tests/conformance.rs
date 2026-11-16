//! Live conformance against a running InfluxDB 1.8, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! database name comes from `INFLUXDB_DB` (created externally) and the
//! endpoint from `INFLUXDB_URL`. Each test drops and recreates the suite
//! measurement via `DROP MEASUREMENT`.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{Repository, Schema};
use rushwind_storage_influxdb::InfluxRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// Binds a repository to `schema` over the live endpoint and resets its
/// measurement.
async fn live_repo(schema: Schema) -> Arc<dyn Repository> {
    let url = std::env::var("INFLUXDB_URL").expect("INFLUXDB_URL must point at a running InfluxDB");
    let database = std::env::var("INFLUXDB_DB").unwrap_or_else(|_| "rushwind".to_owned());
    let repo = InfluxRepo::new(url.clone(), database.clone(), schema);
    // Reset the measurement: the drop is 200-tolerant AND eventually
    // consistent — the query returns before the shard removes the
    // points, so a seed issued immediately reuses the previous run's
    // primary keys (the live suite caught the collision). Poll
    // SHOW MEASUREMENTS until the name is gone.
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/query?db={}", url, database))
        .form(&[("q", format!("DROP MEASUREMENT \"{}\"", repo.schema().table))])
        .send()
        .await;
    for _ in 0..100 {
        let listed = match client
            .post(format!("{}/query?db={}", url, database))
            .form(&[("q", "SHOW MEASUREMENTS".to_string())])
            .send()
            .await
        {
            Ok(response) => response.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        let gone = !listed.contains(&format!("\"{}\"", repo.schema().table));
        if gone {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Arc::new(repo)
}

/// A fresh, empty measurement for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    // The shared suite schema — the JSON column rides the line
    // protocol as its canonical text field and reads back typed, the
    // same shape the offline pins freeze.
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));
    live_repo(schema).await
}

/// A fresh, empty tenant-scoped measurement for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    let mut schema = rushwind_testkit::storage_conformance::tenant_suite_schema();
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));
    live_repo(schema).await
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
