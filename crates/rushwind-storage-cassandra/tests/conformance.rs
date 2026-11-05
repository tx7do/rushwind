//! Live conformance against a running Cassandra 5, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; hosts
//! come from `CASSANDRA_HOSTS` (comma-separated). Each test drops and
//! recreates the suite table for a clean slate.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_cassandra::CassandraRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// Connects to the live cluster and returns a repository bound to `schema`
/// over a fresh, empty table.
async fn live_repo(schema: Schema) -> Arc<dyn Repository> {
    let hosts: Vec<String> = std::env::var("CASSANDRA_HOSTS")
        .expect("CASSANDRA_HOSTS must list Cassandra contact points")
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect();
    let repo = CassandraRepo::connect(&hosts, "rushwind_suite", schema)
        .await
        .expect("connects to Cassandra");
    repo.migrate_drop().await.expect("drops any stale table");
    repo.migrate_create()
        .await
        .expect("creates the suite table");
    Arc::new(repo)
}

/// A fresh, empty table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let mut schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid");
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));
    live_repo(schema).await
}

/// A fresh, empty tenant-scoped table for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    let mut schema = rushwind_testkit::storage_conformance::tenant_suite_schema();
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));
    live_repo(schema).await
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
