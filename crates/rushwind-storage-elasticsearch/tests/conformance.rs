//! Live conformance against a running Elasticsearch, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! endpoint comes from `ELASTICSEARCH_URL`. Each test drops and recreates
//! the suite index for a clean slate.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{Repository, Schema};
use rushwind_storage_elasticsearch::ElasticRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A fresh, empty index for every suite test — unique per test so the
/// parallel suite never shares rows. The table rename keeps the two suite
/// schemas (plain and tenant-scoped) on separate indices, same as the
/// per-test counter keeps tests apart.
async fn fresh_repo_with(schema: Schema) -> Arc<dyn Repository> {
    let url = std::env::var("ELASTICSEARCH_URL")
        .expect("ELASTICSEARCH_URL must point at a running Elasticsearch");
    let mut schema = schema;
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = ElasticRepo::new(url, schema);
    // Reset the index: Elasticsearch deletions are effectively async, so
    // poll until the index is really gone before recreating it — otherwise
    // the recreate is rejected as "already exists" and the fresh mapping
    // never lands.
    let base = format!("http://localhost:9200/{}", repo.schema().table);
    let client = reqwest::Client::new();
    let _ = client.delete(&base).send().await;
    for _ in 0..50 {
        let gone = client
            .get(&base)
            .send()
            .await
            .map(|r| r.status().as_u16() == 404)
            .unwrap_or(true);
        if gone {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    repo.ensure_index()
        .await
        .expect("creates the suite index with mappings");
    Arc::new(repo)
}

/// A fresh, empty index bound to the plain suite schema.
async fn fresh_repo() -> Arc<dyn Repository> {
    fresh_repo_with(rushwind_testkit::storage_conformance::suite_schema()).await
}

/// A fresh, empty tenant-scoped index for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    fresh_repo_with(rushwind_testkit::storage_conformance::tenant_suite_schema()).await
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
