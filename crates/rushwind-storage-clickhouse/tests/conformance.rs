//! Live conformance against a running ClickHouse, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! endpoint comes from `CLICKHOUSE_URL`. Each test drops and recreates the
//! suite table for a clean slate.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{Repository, Schema};
use rushwind_storage_clickhouse::ClickHouseRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A fresh, empty table for every suite test. The table rename keeps the
/// two suite schemas (plain and tenant-scoped) on separate tables, same as
/// the per-test counter keeps tests apart.
async fn fresh_repo_with(schema: Schema) -> Arc<dyn Repository> {
    let url = std::env::var("CLICKHOUSE_URL")
        .expect("CLICKHOUSE_URL must point at a running ClickHouse HTTP endpoint (credentials go inside the URL)");
    let mut schema = schema;
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = ClickHouseRepo::new(url, schema);
    // Reset the table (the engine's drop is 404-tolerant over HTTP errors
    // we ignore; a missing table is the same end state as a dropped one).
    let _ = repo.migrate_drop().await;
    repo.migrate_create()
        .await
        .expect("creates the suite table");
    Arc::new(repo)
}

/// A fresh, empty table bound to the plain suite schema.
async fn fresh_repo() -> Arc<dyn Repository> {
    fresh_repo_with(rushwind_testkit::storage_conformance::suite_schema()).await
}

/// A fresh, empty tenant-scoped table for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    fresh_repo_with(rushwind_testkit::storage_conformance::tenant_suite_schema()).await
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
