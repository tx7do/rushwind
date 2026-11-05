//! Live conformance against a running MongoDB, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! endpoint comes from `MONGODB_URI`. Every test gets its own uniquely-named
//! collection, so the suite's parallel tests never collide.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_mongodb::MongoRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// The collection name the next factory binds to: unique per test so the
/// suite's parallel cases never share one collection.
fn next_table() -> String {
    format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// Connects to a fresh collection bound to `schema` — the shared lifecycle
/// of both factories: drop any stale collection, then create the production
/// primary-key index the conflict semantics lean on.
async fn fresh_collection(uri: &str, schema: rushwind_storage::Schema) -> Arc<dyn Repository> {
    let repo = MongoRepo::connect(uri, "rushwind_suite", schema)
        .await
        .expect("connects to MongoDB");
    // A fresh collection per run: leftover documents from a previous
    // execution would pollute the suite's count/ordering expectations.
    repo.drop_collection()
        .await
        .expect("drops any stale collection");
    repo.ensure_primary_index()
        .await
        .expect("creates the primary key index");
    Arc::new(repo)
}

/// A fresh, empty table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let uri =
        std::env::var("MONGODB_URI").expect("MONGODB_URI must point at a running MongoDB instance");
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.table = next_table();
    fresh_collection(&uri, schema).await
}

/// A fresh, empty tenant-scoped collection for the tenancy suite — same
/// lifecycle as [`fresh_repo`], bound to the tenant schema instead. The
/// tenancy suite's upsert-conflict expectation rides on the same unique
/// primary-key index as the base suite's.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    let uri =
        std::env::var("MONGODB_URI").expect("MONGODB_URI must point at a running MongoDB instance");
    let mut schema = rushwind_testkit::storage_conformance::tenant_suite_schema();
    schema.table = next_table();
    fresh_collection(&uri, schema).await
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
